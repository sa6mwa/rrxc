package anystore_test

import (
	"bytes"
	"crypto/aes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/sa6mwa/rrxc/pkg/anystore"
)

func TestAnyStore_Run_persisted(t *testing.T) {
	f, err := os.CreateTemp("", "anystore-test-run-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := f.Name()
	f.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()

	errTesting := errors.New("this error")
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: true,
		PersistenceFile:   tempfile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Fatal(err)
	}
	if !a.HasKey("hello") {
		t.Fatal("expected key not found in store (outside of Run)")
	}

	err = a.Run(func(as anystore.AnyStore) error {
		if !as.HasKey("hello") {
			t.Error("expected key not found in store")
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		val, err := as.Load(struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := val.(string)
		if !ok {
			t.Fatalf("expected key \"struct{}{}\" with value of type string not found in store")
		}
		if v != "okilidokili" {
			t.Errorf("expected okilidokili, but got %q", v)
		}

		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 2 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		as.Store(struct{}{}, "completely")
		as.Store(struct{}{}, "different")
		as.Delete(struct{}{})
		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 1 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		return errTesting
	})
	if err != errTesting {
		t.Errorf("expected error %v, but got %v", errTesting, err)
	}
	o, err := a.Load(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	expected := "okilidokili"
	if o != expected {
		t.Fatalf("expected key %q with value %q not found in store", "struct{}{}", expected)
	}
	nilVal, err := a.Load("keyNotPresent")
	if err != nil {
		t.Fatal(err)
	}
	if nilVal != nil {
		t.Errorf("expected nil, but got %T", nilVal)
	}
}

func TestAnyStore_Run_persisted_gzip(t *testing.T) {
	f, err := os.CreateTemp("", "anystore-test-run-gzip-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := f.Name()
	f.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()

	errTesting := errors.New("this error")
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence:   true,
		PersistenceFile:     tempfile,
		GZipPersistenceFile: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Fatal(err)
	}
	if !a.HasKey("hello") {
		t.Fatal("expected key not found in store (outside of Run)")
	}

	err = a.Run(func(as anystore.AnyStore) error {
		// Test SetPersistenceFile aswell, if not just for coverage...
		if _, err := as.SetPersistenceFile(tempfile); err != nil {
			t.Error(err)
		}
		// Cover SetEncryptionKey aswell...
		if _, err := as.SetEncryptionKey(anystore.DefaultEncryptionKey); err != nil {
			t.Error(err)
		}
		lenKeys, err := as.Keys()
		if err != nil {
			t.Error(err)
		}
		if len(lenKeys) == 0 {
			t.Errorf("expected more than zero keys")
		}

		if !as.HasKey("hello") {
			t.Error("expected key not found in store")
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		val, err := as.Load(struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := val.(string)
		if !ok {
			t.Fatalf("expected key \"struct{}{}\" with value of type string not found in store")
		}
		if v != "okilidokili" {
			t.Errorf("expected okilidokili, but got %q", v)
		}

		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 2 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		as.Store(struct{}{}, "completely")
		as.Store(struct{}{}, "different")
		as.Delete(struct{}{})
		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 1 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		return errTesting
	})
	if err != errTesting {
		t.Errorf("expected error %v, but got %v", errTesting, err)
	}
	o, err := a.Load(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	expected := "okilidokili"
	if o != expected {
		t.Fatalf("expected key %q with value %q not found in store", "struct{}{}", expected)
	}
	nilVal, err := a.Load("keyNotPresent")
	if err != nil {
		t.Fatal(err)
	}
	if nilVal != nil {
		t.Errorf("expected nil, but got %T", nilVal)
	}
}

func TestAnyStore_Delete(t *testing.T) {
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Error(err)
	}
	if err := a.Store("hola", "mundo"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
	if err := a.Delete("hello"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}
}

func TestAnyStore_Delete_persisted(t *testing.T) {
	fl, err := os.CreateTemp("", "anystore-test-delete-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := fl.Name()
	fl.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: true,
		PersistenceFile:   tempfile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Error(err)
	}
	if err := a.Store("hola", "mundo"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
	if err := a.Delete("hello"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}
	fi, err := os.Stat(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() == 0 {
		t.Errorf("test store %q is of 0 size, expected more", tempfile)
	}
}

func TestAnyStore_Delete_persisted_gzipped(t *testing.T) {
	fl, err := os.CreateTemp("", "anystore-test-delete-gzip-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := fl.Name()
	fl.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()
	secret := anystore.NewKey()
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence:   true,
		PersistenceFile:     tempfile,
		GZipPersistenceFile: true,
		EncryptionKey:       secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Error(err)
	}
	if err := a.Store("hola", "mundo"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
	if err := a.Delete("hello"); err != nil {
		t.Error(err)
	}
	if keys, err := a.Keys(); err != nil {
		t.Error(err)
	} else if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}
	fi, err := os.Stat(tempfile)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() == 0 {
		t.Errorf("test store %q is of 0 size, expected more", tempfile)
	}
}

func TestAnyStore_Run(t *testing.T) {
	errTesting := errors.New("this error")
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store("hello", "world"); err != nil {
		t.Fatal(err)
	}
	if !a.HasKey("hello") {
		t.Fatal("expected key not found in store (outside of Run)")
	}

	err = a.Run(func(as anystore.AnyStore) error {
		if !as.HasKey("hello") {
			t.Error("expected key not found in store")
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		val, err := as.Load(struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := val.(string)
		if !ok {
			t.Fatalf("expected key \"struct{}{}\" with value of type string not found in store")
		}
		if v != "okilidokili" {
			t.Errorf("expected okilidokili, but got %q", v)
		}

		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 2 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		as.Store(struct{}{}, "completely")
		as.Store(struct{}{}, "different")
		as.Delete(struct{}{})
		if l, err := as.Len(); err != nil {
			t.Fatal(err)
		} else if l != 1 {
			t.Errorf("expected Len() == %d, got %d", 2, l)
		}
		if err := as.Store(struct{}{}, "okilidokili"); err != nil {
			t.Fatal(err)
		}
		return errTesting
	})
	if err != errTesting {
		t.Errorf("expected error %v, but got %v", errTesting, err)
	}
	o, err := a.Load(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	expected := "okilidokili"
	if o != expected {
		t.Fatalf("expected key %q with value %q not found in store", "struct{}{}", expected)
	}
	nilVal, err := a.Load("keyNotPresent")
	if err != nil {
		t.Fatal(err)
	}
	if nilVal != nil {
		t.Errorf("expected nil, but got %T", nilVal)
	}
}

func TestAnyStore_GetEncryptionKeyBytes(t *testing.T) {
	expected, err := base64.RawStdEncoding.DecodeString(anystore.DefaultEncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	obtained := a.GetEncryptionKeyBytes()
	if !bytes.Equal(expected, obtained) {
		t.Error("obtained bytes from AnyStore.GetEncryptionKeyBytes() and expected bytes do not match")
	}
}

func TestAnyStore_GetEncryptionKeyBytes_unsafe(t *testing.T) {
	expected, err := base64.RawStdEncoding.DecodeString(anystore.DefaultEncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Run(func(s anystore.AnyStore) error {
		obtained := s.GetEncryptionKeyBytes()
		if !bytes.Equal(expected, obtained) {
			t.Error("obtained bytes from AnyStore.GetEncryptionKeyBytes() and expected bytes do not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func ExampleAnyStore_Store_encrypt() {
	f, err := os.CreateTemp("", "anystore-example-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := f.Name()
	f.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()

	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: true,
		PersistenceFile:   tempfile,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := a.Store("hello", "world"); err != nil {
		fmt.Println(err)
		return
	}

	if v, err := a.Load("hello"); err != nil {
		fmt.Println(err)
		return
	} else {
		val, ok := v.(string)
		if !ok {
			fmt.Println("val is not a string")
			return
		}
		fmt.Println(val)
	}

	if k, err := a.Keys(); err != nil {
		fmt.Println(err)
		return
	} else {
		for _, ky := range k {
			fmt.Println(ky)
		}
	}

	if l, err := a.Len(); err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Println(l)
	}

	// Output:
	// world
	// hello
	// 1
}

func BenchmarkStoreAndLoadPersistence(b *testing.B) {
	f, err := os.CreateTemp("", "anystore-benchmark-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := f.Name()
	f.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()

	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: true,
		PersistenceFile:   tempfile,
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("%s-%d", b.Name(), b.N)
		if err := a.Store(b.N, value); err != nil {
			b.Fatal(err)
		}
		if v, err := a.Load(b.N); err != nil {
			b.Fatal(err)
		} else {
			val, ok := v.(string)
			if !ok {
				b.Fatal("value is not a string")
			}
			if val != value {
				b.Fatal("value does not match expected string")
			}
		}
	}
}

func BenchmarkStoreAndLoadGZippedPersistence(b *testing.B) {
	f, err := os.CreateTemp("", "anystore-benchmark-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	tempfile := f.Name()
	f.Close()
	defer func() {
		os.Remove(tempfile)
		os.Remove(tempfile + ".lock")
	}()

	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence:   true,
		GZipPersistenceFile: true,
		PersistenceFile:     tempfile,
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("%s-%d", b.Name(), b.N)
		if err := a.Store(b.N, value); err != nil {
			b.Fatal(err)
		}
		if v, err := a.Load(b.N); err != nil {
			b.Fatal(err)
		} else {
			val, ok := v.(string)
			if !ok {
				b.Fatal("value is not a string")
			}
			if val != value {
				b.Fatal("value does not match expected string")
			}
		}
	}
}

func BenchmarkStoreAndLoad(b *testing.B) {
	a, err := anystore.NewAnyStore(&anystore.Options{
		EnablePersistence: false,
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("%s-%d", b.Name(), b.N)
		if err := a.Store(b.N, value); err != nil {
			b.Fatal(err)
		}
		if v, err := a.Load(b.N); err != nil {
			b.Fatal(err)
		} else {
			val, ok := v.(string)
			if !ok {
				b.Fatal("value is not a string")
			}
			if val != value {
				b.Fatal("value does not match expected string")
			}
		}
	}
}

func FuzzConcurrentPersistence(f *testing.F) {

	f.Add(1, false, "hello world")

	f.Fuzz(func(t *testing.T, count int, gzip bool, valueToStore string) {
		file, err := os.CreateTemp("", "anystore-concurrent-fuzz-*")
		if err != nil {
			fmt.Println(err)
			return
		}
		tempfile := file.Name()
		file.Close()
		defer func() {
			os.Remove(tempfile)
			os.Remove(tempfile + ".lock")
		}()

		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		go func() {
			defer close(ch1)
			one, err := anystore.NewAnyStore(&anystore.Options{
				EnablePersistence:   true,
				PersistenceFile:     tempfile,
				GZipPersistenceFile: gzip,
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				key := fmt.Sprintf("one-%d", i)
				value := valueToStore
				if err := one.Store(key, value); err != nil {
					t.Fatal(err)
				}
				if v, err := one.Load(key); err != nil {
					t.Fatal(err)
				} else {
					val, ok := v.(string)
					if !ok {
						t.Fatalf("value %q is not a string (expected %q=%q)", val, key, value)
					}
					if val != value {
						t.Fatalf("value %q does not match expected string %q", val, value)
					}
				}
			}
		}()

		go func() {
			defer close(ch2)
			two, err := anystore.NewAnyStore(&anystore.Options{
				EnablePersistence:   true,
				PersistenceFile:     tempfile,
				GZipPersistenceFile: gzip,
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				key := fmt.Sprintf("two-%d", i)
				value := valueToStore
				if err := two.Store(key, value); err != nil {
					t.Fatal(err)
				}
				if v, err := two.Load(key); err != nil {
					t.Fatal(err)
				} else {
					val, ok := v.(string)
					if !ok {
						t.Fatalf("value %q is not a string (expected %q=%q)", val, key, value)
					}
					if val != value {
						t.Fatalf("value %q does not match expected string %q", val, value)
					}
				}
			}
		}()

		<-ch1
		<-ch2
	})

}

func TestEncrypt(t *testing.T) {
	encTestFunc := func(key []byte, data []byte) {
		t.Logf("Testing %d bytes long key", len(key))
		mac := hmac.New(sha256.New, key)
		block, err := aes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		if aes.BlockSize != block.BlockSize() {
			t.Errorf("aes.BlockSize != block.BlockSize(), but %d and %d respectively", aes.BlockSize, block.BlockSize())
		}
		t.Logf("mac.Size() = %d", mac.Size())
		t.Logf("aes.BlockSize = %d", aes.BlockSize)
		t.Logf("block.BlockSize = %d", block.BlockSize())
		encrypted, err := anystore.Encrypt(key, data)
		if err != nil {
			t.Fatal(err)
		}
		if len(encrypted) < mac.Size()+aes.BlockSize {
			t.Fatalf("length of enciphered data is less than mac.Size()+aes.BlockSize (want>%d, got %d)", mac.Size()+aes.BlockSize, len(encrypted))
		}
		// Get HMAC from cipher-text
		encryptedHMAC := encrypted[:mac.Size()]
		message := encrypted[mac.Size():]
		//iv := encrypted[mac.Size() : mac.Size()+aes.BlockSize]
		//cipherText := encrypted[mac.Size()+aes.BlockSize:]
		if _, err := mac.Write(message); err != nil {
			t.Fatal(err)
		}
		if !hmac.Equal(encryptedHMAC, mac.Sum(nil)) {
			t.Fatal(anystore.ErrHMACValidationFailed)
		}
		// Decrypt must also work
		decrypted, err := anystore.Decrypt(key, encrypted)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, decrypted) {
			t.Logf("origidata=%v", data)
			t.Logf("decrypted=%v", decrypted)
			t.Fatal("original data and decrypted data (from encryption) does not match")
		}
	}

	key, err := anystore.ToBinaryEncryptionKey(anystore.NewKey())
	if err != nil {
		t.Fatal(err)
	}

	if len(key) != 32 {
		t.Fatalf("expected NewKey() to produce a 32 byte long key, but got %d bytes", len(key))
	}

	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	keys := [][]byte{key, key[:24], key[:16]}
	for _, k := range keys {
		encTestFunc(k, data)
	}
}

func TestDecrypt(t *testing.T) {
	decrypTestFunc := func(key []byte, data []byte) {
		t.Logf("Testing %d bytes long key", len(key))
		mac := hmac.New(sha256.New, key)
		if len(data) < mac.Size()+aes.BlockSize {
			t.Fatalf("length of cipher data is less than mac.Size()+aes.BlockSize (want>%d, got %d)", mac.Size()+aes.BlockSize, len(data))
		}
		messageWithIV := data[mac.Size():]
		t.Logf("mac.Size() == %d", mac.Size())
		t.Logf("data is %d bytes long, w/o HMAC == %d", len(data), len(messageWithIV))
		messageHMAC := data[:mac.Size()]
		if _, err := mac.Write(messageWithIV); err != nil {
			t.Fatal(err)
		}
		if !hmac.Equal(messageHMAC, mac.Sum(nil)) {
			t.Fatal(anystore.ErrHMACValidationFailed)
		}
	}

	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	key, err := anystore.ToBinaryEncryptionKey(anystore.NewKey())
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range [][]byte{key, key[:24], key[:16]} {
		ciphered, err := anystore.Encrypt(k, data)
		if err != nil {
			t.Fatal(err)
		}
		decrypTestFunc(k, ciphered)
	}

}
