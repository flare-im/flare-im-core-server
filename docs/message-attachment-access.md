# Message-bound private attachment reads

Private objects remain private. Knowing a file ID, being in the same application,
or receiving a client-supplied URL is not read authority. The existing owner and
explicit read-grant paths remain unchanged.

The normal file URL request may additionally identify a conversation and its
server sequence. For a non-owner, Media asks Storage Reader for precisely that
persisted message, forwarding the original verified User context. Reader applies
the authenticated user's current participation/history floor, deletion and
retention visibility. Media additionally requires:

- the returned conversation/sequence equal the requested reference;
- accepted/persisted, non-recalled/non-deleted typed content;
- the persisted sender is the media's recorded authenticated owner;
- the exact file ID occurs in a supported typed attachment, never a URL, text or
  arbitrary extension/metadata string.

Recipient URLs last at most 60 seconds. Every renewal rechecks authoritative
visibility. Dependency failure fails closed. No database writes, public-object
conversion, per-recipient grant mutation, Staff impersonation or Social policy
codes enter this path. File, image/image-group, video/cover and audio are generic
communication types. Forwarded/rich-document references are not inferred from
untrusted embedded IDs; a future provenance contract must cover those explicitly.

Console inspection uses the existing message-detail GET with optional
`attachment_id`. The Core admin boundary requires the original verified Staff
assertion for `core:messages:read` in one concrete application. The ID must occur
in that message's projected typed attachments. Media independently forwards the
same Staff assertion to Reader's admin search for that exact conversation/sequence,
then checks the persisted sender/owner and content visibility again. It does not
impersonate a User or reuse the owner's/public-object/grant shortcut. Expired,
purged, hidden and recalled messages cannot authorize this path. Presigned URLs
last at most 60 seconds; Console accepts HTTPS only. Message responses are no-store.

The Console's readable body is plain text plus typed attachment metadata, never
HTML, opaque payloads or sender-supplied URLs. Its media modal and download helper
clear or cancel on scope/permission changes; download omits cookies, rejects
redirects and refuses files above 128 MiB without a navigation fallback.

Social's per-application Composer policy remains in Social Hook/SDK/App. Disabling
new sends does not remove already-authorized history or stored attachments.
