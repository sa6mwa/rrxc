package anystore

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (
	ErrNilThing      error = errors.New("nil pointer to thing to stash or unstash")
	ErrEmptyKey      error = errors.New("stash key can not be empty (zero-length key)")
	ErrNotAPointer   error = errors.New("stash configuration does not point to thing, need a pointer")
	ErrThingNotFound error = errors.New("thing not found in stash")
	ErrTypeMismatch  error = errors.New("type-mismatch between thing and default thing")
	ErrMissingReader error = errors.New("missing filename (or io.Reader) to load persisted data from")
	ErrMissingWriter error = errors.New("missing filename (or io.Writer) to persist data to")
)

// StashConfig instructs how functions anystore.Stash and anystore.Unstash
// should save/load a "stash". If Reader is not nil and File is not an empty
// string, Reader will be preferred over File when executing Unstash. If Writer
// is not nil and File is not an empty string when executing Stash, the file
// will be written first, then written to through the io.Writer (both will be
// written to). Writer.Close() is deferred early, Stash always closes the writer
// on success and failure. If File is an empty string (== "") and Writer is not
// nil, Stash will only write to the io.Writer.
type StashConfig struct {
	// AnyStore DB file, if empty, use Reader/Writer.
	File string

	// If nil, use File for Unstash, if not, prefer Reader over File.
	Reader io.Reader

	// If nil, use File for Stash, if not, write to both Writer and File
	// (if File is not an empty string).
	Writer io.WriteCloser

	// GZip data before encryption.
	GZip bool

	// 16, 24 or 32 byte long base64-encoded string.
	EncryptionKey string

	// Key name where to store Thing.
	Key string

	// Thing is usually a struct with data, properties, configuration,
	// etc. Must be a pointer. On Unstash (and EditThing), Thing should
	// be zeroed (new/empty) or the result of underlying gob.Decode is
	// unpredictable.
	Thing any

	// If Unstash get os.ErrNotExist or key is missing, use this as
	// default Thing if not nil. Must be a pointer.
	DefaultThing any

	// Editor to use to edit Thing as JSON.
	Editor string
}

// "stash, verb. to put (something of future use or value) in a safe or secret
// place"
//
// Unstash loads a "Thing" from a place specified in a StashConfig,
// usually an AnyStoreDB file, into the object pointed to by the Thing
// field in conf. Thing should be an uninitialized/new/empty object
// for predictable results, full overwrite of any present value is not
// guaranteed.
//
// The Stash and Unstash functions also support io.Reader and
// io.Writer (io.WriteCloser). Reader/writer is essentially an
// in-memory version of the physical DB file, Unstash does io.ReadAll
// into memory in order to decrypt and de-GOB the data. A previous
// file-Stash command can be Unstashed via the io.Reader. Unstash
// prefers io.Reader when both StashConfig.File and StashConfig.Reader
// are defined.
//
// StashConfig instructs how functions anystore.Stash and anystore.Unstash
// should save/load a "stash". If Reader is not nil and File is not an empty
// string, Reader will be preferred over File when executing Unstash. If Writer
// is not nil and File is not an empty string when executing Stash, the file
// will be written first, then written to through the io.Writer (both will be
// written to). Writer.Close() is deferred early, Stash always closes the writer
// on success and failure. If File is an empty string (== "") and Writer is not
// nil, Stash will only write to the io.Writer.
func Unstash(conf *StashConfig) error {
	if conf.Thing == nil {
		return ErrNilThing
	}
	if conf.Key == "" {
		return ErrEmptyKey
	}
	if conf.File == "" && conf.Reader == nil {
		return ErrMissingReader
	}
	options := Options{
		EnablePersistence:   true,
		PersistenceFile:     conf.File,
		GZipPersistenceFile: conf.GZip,
		EncryptionKey:       conf.EncryptionKey,
	}
	// If we have an io.Reader, prefer it above File.
	if conf.Reader != nil {
		options.EnablePersistence = false
	}
	a, err := NewAnyStore(&options)
	if err != nil {
		return err
	}
	defer a.Close()
	var gobbedThing any
	if conf.Reader != nil {
		// Read encrypted anyMap
		kv := make(anyMap)
		data, err := io.ReadAll(conf.Reader)
		if err != nil {
			return err
		}
		decrypted, err := Decrypt(a.GetEncryptionKeyBytes(), data)
		if err != nil {
			return err
		}
		var in *gob.Decoder
		if conf.GZip {
			gzipReader, err := gzip.NewReader(bytes.NewReader(decrypted))
			if err != nil {
				if errors.Is(err, gzip.ErrHeader) {
					return fmt.Errorf("%w (perhaps persistence is not gzipped?)", err)
				}
				return err
			}
			in = gob.NewDecoder(gzipReader)
		} else {
			in = gob.NewDecoder(bytes.NewReader(decrypted))
		}
		if err := in.Decode(&kv); err != nil {
			if strings.Contains(err.Error(), "encoded unsigned integer out of range") && conf.GZip {
				return fmt.Errorf("%w (perhaps persistence is gzipped?)", err)
			}
			return err
		}
		var ok bool
		gobbedThing, ok = kv[conf.Key]
		if !ok {
			return ErrThingNotFound
		}
	} else {
		// Load key from PersistenceFile instead.
		var err error
		gobbedThing, err = a.Load(conf.Key)
		if err != nil {
			return err
		}
	}
	// GOB encoded thing came from either file or io.Reader.
	thing, ok := gobbedThing.([]byte)
	if !ok {
		if conf.DefaultThing != nil {
			if reflect.TypeOf(conf.Thing) != reflect.TypeOf(conf.DefaultThing) {
				return fmt.Errorf("%w: %s != %s", ErrTypeMismatch, reflect.TypeOf(conf.DefaultThing), reflect.TypeOf(conf.Thing))
			}
			if reflect.TypeOf(conf.Thing).Kind() != reflect.Pointer || reflect.TypeOf(conf.DefaultThing).Kind() != reflect.Pointer {
				return ErrNotAPointer
			}
			reflect.Indirect(reflect.ValueOf(conf.Thing)).Set(reflect.Indirect(reflect.ValueOf(conf.DefaultThing)))
			return nil
		}
		return ErrThingNotFound
	}
	g := gob.NewDecoder(bytes.NewReader(thing))
	// Decode into wherever StashConfig.Thing is pointing to.
	if err := g.Decode(conf.Thing); err != nil {
		return fmt.Errorf("gob.Decode(Thing): %w", err)
	}
	return nil
}

// "stash, verb. to put (something of future use or value) in a safe or secret
// place"
//
// Stash stores a "Thing" according to a StashConfig, usually an AnyStore DB
// file, but Stash and Unstash can also be used with an io.Writer
// (io.WriteCloser) and an io.Reader for arbitrary stashing/unstashing. Stash
// always closes the writer on exit (why it's an io.WriteCloser). The
// reader/writers are essentially in-memory versions of the physical DB file,
// Unstash does io.ReadAll into memory in order to decrypt and de-GOB it.
//
// StashConfig instructs how functions anystore.Stash and anystore.Unstash
// should save/load a "stash". If Reader is not nil and File is not an empty
// string, Reader will be preferred over File when executing Unstash. If Writer
// is not nil and File is not an empty string when executing Stash, the file
// will be written first, then written to through the io.Writer (both will be
// written to). Writer.Close() is deferred early, Stash always closes the writer
// on success and failure. If File is an empty string (== "") and Writer is not
// nil, Stash will only write to the io.Writer.
func Stash(conf *StashConfig) error {
	if conf.Writer != nil {
		defer conf.Writer.Close()
	}
	value := reflect.ValueOf(conf.Thing)
	if value.Type().Kind() != reflect.Pointer {
		return ErrNotAPointer
	}
	if value.IsNil() {
		return ErrNilThing
	}
	if conf.Key == "" {
		return ErrEmptyKey
	}
	if conf.File == "" && conf.Writer == nil {
		return ErrMissingWriter
	}

	options := Options{
		PersistenceFile:     conf.File,
		GZipPersistenceFile: conf.GZip,
		EncryptionKey:       conf.EncryptionKey,
	}
	if conf.File == "" {
		options.EnablePersistence = false
	} else {
		options.EnablePersistence = true
	}

	a, err := NewAnyStore(&options)
	if err != nil {
		return err
	}
	defer a.Close()

	// Use gob to store the struct (or other value) instead of re-inventing
	// dereference of all pointers. It is also unlikely that the interface stored
	// is registered with gob in the downstream anystore package.
	var thing bytes.Buffer
	g := gob.NewEncoder(&thing)
	if err := g.Encode(conf.Thing); err != nil {
		return fmt.Errorf("gob.Encode of StashConfig.Thing: %w", err)
	}
	// Persist to file if filename was not an empty string.
	if conf.File != "" {
		if err := a.Store(conf.Key, thing.Bytes()); err != nil {
			return err
		}
	}
	// If conf.Writer was given, also write to the io.Writer, but this has to be
	// emulated (AnyStore does not implement io.Writer or io.Reader).
	if conf.Writer != nil {
		kv := make(anyMap)
		kv[conf.Key] = thing.Bytes()
		var output bytes.Buffer
		var out *gob.Encoder
		var gzipWriter *gzip.Writer
		if conf.GZip {
			gzipWriter = gzip.NewWriter(&output)
			out = gob.NewEncoder(gzipWriter)
		} else {
			out = gob.NewEncoder(&output)
		}
		if err := out.Encode(kv); err != nil {
			if gzipWriter != nil {
				gzipWriter.Close()
			}
			return err
		}
		if gzipWriter != nil {
			gzipWriter.Close()
		}
		encrypted, err := Encrypt(a.GetEncryptionKeyBytes(), output.Bytes())
		if err != nil {
			return err
		}
		if n, err := conf.Writer.Write(encrypted); err != nil {
			return err
		} else if n != len(encrypted) {
			return ErrWroteTooLittle
		}
	}
	return nil
}

// Wrap bytes.Buffer to implement io.Closer
type BytesBufferWriteCloser struct {
	bytes.Buffer
}

// Close implements io.Closer on wrapped bytes.Buffer
func (b *BytesBufferWriteCloser) Close() error {
	return nil
}

// NewStashReader Stashes according to supplied StashConfig, except only to an
// internal io.WriteCloser (not to the File). The Writer is over-ridden and
// pointed to an internal wrapped bytes.Buffer which it returns, unless there
// was an error. The wrapped bytes.Buffer implements a fake io.Closer making it
// an io.WriteCloser.
func NewStashReader(conf *StashConfig) (*BytesBufferWriteCloser, error) {
	var buf BytesBufferWriteCloser
	newConf := &StashConfig{
		File:          "",
		Reader:        nil,
		Writer:        &buf,
		GZip:          conf.GZip,
		EncryptionKey: conf.EncryptionKey,
		Key:           conf.Key,
		Thing:         conf.Thing,
		DefaultThing:  conf.DefaultThing,
		Editor:        conf.Editor,
	}
	if err := Stash(newConf); err != nil {
		return nil, err
	}
	return &buf, nil
}
