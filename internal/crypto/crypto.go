package crypto

import (
	"fmt"
	"io"
	"os"

	"filippo.io/age"
)

// Encryptor wraps a writer with encryption and unwraps a reader with decryption.
// NoopEncryptor is used when encryption is disabled — zero overhead.
type Encryptor interface {
	// Encrypt wraps w so that anything written is encrypted.
	// The caller must Close() the returned WriteCloser to flush the final block.
	Encrypt(w io.Writer) (io.WriteCloser, error)

	// Decrypt wraps r so that reads are transparently decrypted.
	Decrypt(r io.Reader) (io.Reader, error)
}

// --- NoopEncryptor -------------------------------------------------------

// NoopEncryptor is a transparent passthrough used when --encrypt is not set.
type NoopEncryptor struct{}

func (NoopEncryptor) Encrypt(w io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{w}, nil
}

func (NoopEncryptor) Decrypt(r io.Reader) (io.Reader, error) {
	return r, nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// --- PassphraseEncryptor -------------------------------------------------

// PassphraseEncryptor uses filippo.io/age scrypt KDF.
// Use when a single shared secret is sufficient.
type PassphraseEncryptor struct {
	passphrase string
}

func NewPassphraseEncryptor(passphrase string) *PassphraseEncryptor {
	return &PassphraseEncryptor{passphrase: passphrase}
}

func (e *PassphraseEncryptor) Encrypt(w io.Writer) (io.WriteCloser, error) {
	recipient, err := age.NewScryptRecipient(e.passphrase)
	if err != nil {
		return nil, fmt.Errorf("creating scrypt recipient: %w", err)
	}
	wc, err := age.Encrypt(w, recipient)
	if err != nil {
		return nil, fmt.Errorf("initialising age encryption: %w", err)
	}
	return wc, nil
}

func (e *PassphraseEncryptor) Decrypt(r io.Reader) (io.Reader, error) {
	identity, err := age.NewScryptIdentity(e.passphrase)
	if err != nil {
		return nil, fmt.Errorf("creating scrypt identity: %w", err)
	}
	dr, err := age.Decrypt(r, identity)
	if err != nil {
		return nil, fmt.Errorf("initialising age decryption: %w", err)
	}
	return dr, nil
}

// --- KeyPairEncryptor ----------------------------------------------------

// KeyPairEncryptor uses filippo.io/age X25519 public/private key pairs.
// Encrypt with a recipient's public key; only the private key holder can decrypt.
type KeyPairEncryptor struct {
	recipientFile string // path to .pub key file (for encrypt)
	identityFile  string // path to private key file (for decrypt)
}

func NewKeyPairEncryptor(recipientFile, identityFile string) *KeyPairEncryptor {
	return &KeyPairEncryptor{recipientFile: recipientFile, identityFile: identityFile}
}

func (e *KeyPairEncryptor) Encrypt(w io.Writer) (io.WriteCloser, error) {
	pubKeyData, err := os.ReadFile(e.recipientFile)
	if err != nil {
		return nil, fmt.Errorf("reading recipient key %q: %w", e.recipientFile, err)
	}

	recipients, err := age.ParseRecipients(
		// age.ParseRecipients accepts a reader of lines like "age1..."
		newStringReader(string(pubKeyData)),
	)
	if err != nil {
		return nil, fmt.Errorf("parsing recipient key: %w", err)
	}

	wc, err := age.Encrypt(w, recipients...)
	if err != nil {
		return nil, fmt.Errorf("initialising age encryption: %w", err)
	}
	return wc, nil
}

func (e *KeyPairEncryptor) Decrypt(r io.Reader) (io.Reader, error) {
	keyData, err := os.ReadFile(e.identityFile)
	if err != nil {
		return nil, fmt.Errorf("reading identity key %q: %w", e.identityFile, err)
	}

	identities, err := age.ParseIdentities(newStringReader(string(keyData)))
	if err != nil {
		return nil, fmt.Errorf("parsing identity key: %w", err)
	}

	dr, err := age.Decrypt(r, identities...)
	if err != nil {
		return nil, fmt.Errorf("initialising age decryption: %w", err)
	}
	return dr, nil
}

// newStringReader returns an io.Reader for a string, used for age key parsing.
func newStringReader(s string) io.Reader {
	return &stringReader{s: s}
}

type stringReader struct {
	s   string
	pos int
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}
