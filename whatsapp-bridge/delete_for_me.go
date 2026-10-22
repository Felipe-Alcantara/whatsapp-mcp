package main

// "Delete for me": remove messages from this WhatsApp account only.
//
// WhatsApp implements it as an app state mutation (collection regular_high,
// index "deleteMessageForMe") that the phone and every linked device apply, so
// it works for messages of any sender and any age. whatsmeow ships the index
// constant and the protobuf for it but no Build helper, hence
// buildDeleteMessageForMePatch below. The other participants keep their copies.
//
// Deleting for everyone (revoking) is a different, send-side operation and is
// not handled here.
//
// Layout: deleteMessagesForMe is the transport-independent core. It only needs
// the three bridge capabilities in deleteForMeDeps, so it can be driven by the
// HTTP handler below, by another caller, or by a test with fakes.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// maxDeleteForMeBatch caps the messages one request may delete. Keep in sync
// with MAX_DELETE_BATCH in whatsapp-mcp-server/whatsapp.py.
const maxDeleteForMeBatch = 200

// DeleteForMeMessage addresses a message by its key, for messages the bridge
// never stored (history sync is partial and some message types are skipped, so
// the phone can show messages that have no row in messages.db).
type DeleteForMeMessage struct {
	ID     string `json:"id"`
	FromMe bool   `json:"from_me"`
	// Timestamp is RFC 3339. WhatsApp matches the message on chat + ID + fromMe,
	// so it only needs to be close; it defaults to now.
	Timestamp string `json:"timestamp,omitempty"`
}

// DeleteForMeRequest is the request body for the /api/delete-for-me endpoint.
// MessageIDs are looked up in messages.db; Messages carry their own key.
type DeleteForMeRequest struct {
	ChatJID    string               `json:"chat_jid"`
	MessageIDs []string             `json:"message_ids,omitempty"`
	Messages   []DeleteForMeMessage `json:"messages,omitempty"`
}

// storedMessageRef is what an app state patch needs to address a message.
// MediaType is only known for stored messages and locates the downloaded file.
type storedMessageRef struct {
	ID        string
	Timestamp time.Time
	FromMe    bool
	MediaType string
}

// GetMessageRefs returns the stored messages among ids in chatJID, keyed by ID.
// IDs that are not stored are simply absent from the result.
func (store *MessageStore) GetMessageRefs(chatJID string, ids []string) (map[string]storedMessageRef, error) {
	refs := make(map[string]storedMessageRef, len(ids))
	if len(ids) == 0 {
		return refs, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, 1+len(ids))
	args = append(args, chatJID)
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	rows, err := store.db.Query(
		`SELECT id, timestamp, is_from_me, COALESCE(media_type, '') FROM messages WHERE chat_jid = ? AND id IN (`+strings.Join(placeholders, ",")+`)`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, mediaType string
		var rawTimestamp any
		var fromMe bool
		if err := rows.Scan(&id, &rawTimestamp, &fromMe, &mediaType); err != nil {
			return nil, err
		}
		ts := anchorTime(rawTimestamp)
		if ts.IsZero() {
			return nil, fmt.Errorf("message %q has an unreadable timestamp %v", id, rawTimestamp)
		}
		refs[id] = storedMessageRef{ID: id, Timestamp: ts, FromMe: fromMe, MediaType: mediaType}
	}
	return refs, rows.Err()
}

// DeleteMessages removes the given messages from the local store. Unlike
// MarkMessageDeleted (a retraction by the sender, which keeps the content), a
// delete-for-me erases the row: the user asked for the message to be gone.
func (store *MessageStore) DeleteMessages(chatJID string, ids []string) error {
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, id := range ids {
		if _, err := tx.Exec(`DELETE FROM messages WHERE id = ? AND chat_jid = ?`, id, chatJID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// buildDeleteMessageForMePatch builds one app state patch deleting every ref in
// chat for this account. The mutation index is
// [deleteMessageForMe, chat, messageID, fromMe, participant]; the participant
// slot is "0" for DMs and for our own messages, the only cases the core
// accepts. WhatsApp matches the message on chat + ID + fromMe, so the timestamp
// only needs to be close.
func buildDeleteMessageForMePatch(chat types.JID, refs []storedMessageRef) appstate.PatchInfo {
	mutations := make([]appstate.MutationInfo, 0, len(refs))
	for _, ref := range refs {
		fromMe := "0"
		if ref.FromMe {
			fromMe = "1"
		}
		mutations = append(mutations, appstate.MutationInfo{
			Index:   []string{appstate.IndexDeleteMessageForMe, chat.String(), ref.ID, fromMe, "0"},
			Version: 3,
			Value: &waSyncAction.SyncActionValue{
				DeleteMessageForMeAction: &waSyncAction.DeleteMessageForMeAction{
					DeleteMedia:      proto.Bool(true),
					MessageTimestamp: proto.Int64(ref.Timestamp.Unix()),
				},
			},
		})
	}
	return appstate.PatchInfo{Type: appstate.WAPatchRegularHigh, Mutations: mutations}
}

// deleteForMeDeps are the bridge capabilities the operation needs. They are
// injected so the connected path can be exercised without a live client.
type deleteForMeDeps struct {
	isConnected  func() bool
	resolveChat  func(chat string) (types.JID, error)
	sendAppState func(ctx context.Context, patch appstate.PatchInfo) error
}

func newDeleteForMeDeps(client *whatsmeow.Client) deleteForMeDeps {
	return deleteForMeDeps{
		isConnected: client.IsConnected,
		resolveChat: func(chat string) (types.JID, error) { return resolveRecipientJID(client, chat) },
		sendAppState: func(ctx context.Context, patch appstate.PatchInfo) error {
			return client.SendAppState(ctx, patch)
		},
	}
}

// deleteForMeError pairs a failure with the HTTP status that describes it.
type deleteForMeError struct {
	Status  int
	Message string
}

func (e *deleteForMeError) Error() string { return e.Message }

// deleteForMeResult reports what the core did.
type deleteForMeResult struct {
	Deleted int
	// MediaRemoved counts the downloaded media files removed from disk.
	MediaRemoved int
	// Warning is set when the patch went out but the local cleanup failed.
	Warning string
}

// removeDownloadedMedia deletes the media files the bridge downloaded for refs.
// The path comes from mediaDownloadStorePaths, the helper the download itself
// uses, so it is confined to store/. A file that was never downloaded is fine.
func removeDownloadedMedia(chatKey string, refs []storedMessageRef) int {
	removed := 0
	for _, ref := range refs {
		if ref.MediaType == "" || ref.MediaType == "reaction" {
			continue
		}
		_, localPath, _, err := mediaDownloadStorePaths(chatKey, ref.MediaType, ref.ID, ref.Timestamp)
		if err != nil {
			continue // an ID or type that can't form a store path was never downloaded
		}
		switch err := os.Remove(localPath); {
		case err == nil:
			removed++
		case !errors.Is(err, fs.ErrNotExist):
			fmt.Printf("Warning: failed to remove media file %s: %v\n", localPath, err)
		}
	}
	return removed
}

// deleteMessagesForMe deletes ids from chatKey for this account. chatKey is the
// storage form of the chat JID (phone JID for DMs). Validation against the
// store is all-or-nothing and happens before any external side effect.
func deleteMessagesForMe(ctx context.Context, deps deleteForMeDeps, store *MessageStore, chatKey string, ids []string, explicit []storedMessageRef) (deleteForMeResult, *deleteForMeError) {
	chat, err := types.ParseJID(chatKey)
	if err != nil || chat.User == "" || chat.Server == "" {
		return deleteForMeResult{}, &deleteForMeError{http.StatusBadRequest, "Invalid chat_jid"}
	}

	stored, err := store.GetMessageRefs(chatKey, ids)
	if err != nil {
		return deleteForMeResult{}, &deleteForMeError{http.StatusInternalServerError, fmt.Sprintf("Failed to look up messages: %v", err)}
	}
	refs := make([]storedMessageRef, 0, len(ids)+len(explicit))
	var missing []string
	for _, id := range ids {
		ref, ok := stored[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		refs = append(refs, ref)
	}
	if len(missing) > 0 {
		return deleteForMeResult{}, &deleteForMeError{http.StatusNotFound, fmt.Sprintf("messages not found in chat: %s", strings.Join(missing, ", "))}
	}
	refs = append(refs, explicit...)

	if chat.Server == types.GroupServer {
		for _, ref := range refs {
			if !ref.FromMe {
				// The participant slot of the mutation index needs the sender in
				// the form the phone uses (PN or LID), which is not verified yet.
				return deleteForMeResult{}, &deleteForMeError{http.StatusBadRequest, "deleting other participants' group messages is not supported yet"}
			}
		}
	}

	if !deps.isConnected() {
		return deleteForMeResult{}, &deleteForMeError{http.StatusServiceUnavailable, "WhatsApp client is not connected. Please wait for reconnection."}
	}

	// MCP storage uses phone-form chat JIDs; address the chat the way the phone
	// does (PN -> LID for migrated DMs), as /api/mark-read does.
	appChat, err := deps.resolveChat(chatKey)
	if err != nil || appChat.User == "" || appChat.Server == "" {
		return deleteForMeResult{}, &deleteForMeError{http.StatusBadRequest, "Invalid chat_jid"}
	}

	if err := deps.sendAppState(ctx, buildDeleteMessageForMePatch(appChat, refs)); err != nil {
		return deleteForMeResult{}, &deleteForMeError{http.StatusInternalServerError, fmt.Sprintf("Failed to delete messages: %v", err)}
	}

	// An explicit ref may still have a row (the caller can mix both forms), so
	// clean up every ID; deleting a row that isn't there is a no-op.
	allIDs := make([]string, 0, len(refs))
	for _, ref := range refs {
		allIDs = append(allIDs, ref.ID)
	}
	result := deleteForMeResult{Deleted: len(refs)}
	if err := store.DeleteMessages(chatKey, allIDs); err != nil {
		// The patch already went out; report it rather than failing the call.
		fmt.Printf("Warning: failed to remove deleted messages from the local store for %s: %v\n", chatKey, err)
		result.Warning = "local store cleanup failed"
	}
	result.MediaRemoved = removeDownloadedMedia(chatKey, refs)
	return result, nil
}

// normalizeDeleteRequest validates the message selection of a request: it trims
// and de-duplicates the stored IDs (keeping their order) and turns the explicit
// messages into refs. The returned string is a client-facing reason when the
// selection is unusable.
func normalizeDeleteRequest(req DeleteForMeRequest, now time.Time) ([]string, []storedMessageRef, string) {
	if len(req.MessageIDs)+len(req.Messages) == 0 {
		return nil, nil, "message_ids or messages are required"
	}
	if len(req.MessageIDs)+len(req.Messages) > maxDeleteForMeBatch {
		return nil, nil, fmt.Sprintf("at most %d messages per request", maxDeleteForMeBatch)
	}
	ids := make([]string, 0, len(req.MessageIDs))
	seen := make(map[string]bool, len(req.MessageIDs)+len(req.Messages))
	for _, id := range req.MessageIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, nil, "message_ids must not contain empty values"
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	explicit := make([]storedMessageRef, 0, len(req.Messages))
	for _, m := range req.Messages {
		id := strings.TrimSpace(m.ID)
		if id == "" {
			return nil, nil, "messages must not contain an empty id"
		}
		if seen[id] {
			return nil, nil, fmt.Sprintf("message %q is listed more than once", id)
		}
		seen[id] = true
		ts := now
		if m.Timestamp != "" {
			parsed, err := time.Parse(time.RFC3339, m.Timestamp)
			if err != nil {
				return nil, nil, fmt.Sprintf("timestamp of message %q must be RFC 3339", id)
			}
			ts = parsed
		}
		explicit = append(explicit, storedMessageRef{ID: id, Timestamp: ts, FromMe: m.FromMe})
	}
	return ids, explicit, ""
}

func deleteForMeHandler(deps deleteForMeDeps, store *MessageStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req DeleteForMeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.ChatJID == "" {
			http.Error(w, "chat_jid is required", http.StatusBadRequest)
			return
		}
		ids, explicit, reason := normalizeDeleteRequest(req, time.Now())
		if reason != "" {
			http.Error(w, reason, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		result, derr := deleteMessagesForMe(context.Background(), deps, store, req.ChatJID, ids, explicit)
		if derr != nil {
			w.WriteHeader(derr.Status)
			_ = json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: derr.Message})
			return
		}

		msg := fmt.Sprintf("%d message(s) deleted for me", result.Deleted)
		if result.MediaRemoved > 0 {
			msg += fmt.Sprintf(", %d media file(s) removed from disk", result.MediaRemoved)
		}
		if result.Warning != "" {
			msg += " (" + result.Warning + ")"
		}
		fmt.Printf("← /api/delete-for-me chat=%q deleted=%d\n", req.ChatJID, result.Deleted)
		_ = json.NewEncoder(w).Encode(SendMessageResponse{Success: true, Message: msg})
	}
}

func registerDeleteForMeEndpoint(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client, messageStore *MessageStore) {
	mux.HandleFunc("/api/delete-for-me", auth(deleteForMeHandler(newDeleteForMeDeps(client), messageStore)))
}
