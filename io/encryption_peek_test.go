package io

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/stretchr/testify/require"
)

// buildFakeParquet creates a temp file with structure: [footerBytes][uint32LE(len)][magic].
func buildFakeParquet(t *testing.T, footerBytes []byte, magic string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "fake*.parquet")
	require.NoError(t, err)
	_, err = f.Write(footerBytes)
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.LittleEndian, uint32(len(footerBytes))))
	_, err = f.Write([]byte(magic))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestReadEncryptionKeyHints(t *testing.T) {
	t.Parallel()
	testCases := map[string]struct {
		uri             string
		setup           func(t *testing.T) string
		wantFooterKM    string
		wantColumnCount int
		wantColumnPath  string
		wantColumnKM    string
		wantNil         bool
		errMsg          string
	}{
		"non-existent": {
			uri:    "file/does/not/exist",
			errMsg: "no such file or directory",
		},
		"unencrypted": {
			uri:     "../testdata/good.parquet",
			wantNil: true,
		},
		// PARE (encrypted footer): FooterKeyMetadata is in the unencrypted FileCryptoMetaData
		"encrypted-footer-pare": {
			uri:          encryptedFooterURI,
			wantFooterKM: "a2Y=",
		},
		"uniform-pare": {
			uri:          encryptedUniformURI,
			wantFooterKM: "a2Y=",
		},
		// PAR1 (plaintext footer): column KeyMetadata is in the plaintext footer CryptoMetadata;
		// FooterSigningKeyMetadata is also in the plaintext footer for PAR1 files
		"encrypted-columns-par1": {
			uri:             encryptedColumnURI,
			wantFooterKM:    "a2Y=",
			wantColumnCount: 2,
			wantColumnPath:  "float_field",
			wantColumnKM:    "a2My",
		},
		// crafted files for error and edge-case paths
		"file-too-small": {
			setup: func(t *testing.T) string {
				f, err := os.CreateTemp(t.TempDir(), "tiny*.parquet")
				require.NoError(t, err)
				_, err = f.Write([]byte{0x01, 0x02, 0x03, 0x04})
				require.NoError(t, err)
				require.NoError(t, f.Close())
				return f.Name()
			},
			errMsg: "file too small to be a parquet file",
		},
		"invalid-footer-length": {
			setup: func(t *testing.T) string {
				// footer_len=1000 but file is only 9 bytes total
				f, err := os.CreateTemp(t.TempDir(), "badfooter*.parquet")
				require.NoError(t, err)
				_, err = f.Write([]byte{0x00})
				require.NoError(t, err)
				require.NoError(t, binary.Write(f, binary.LittleEndian, uint32(1000)))
				_, err = f.Write([]byte("PAR1"))
				require.NoError(t, err)
				require.NoError(t, f.Close())
				return f.Name()
			},
			errMsg: "invalid parquet footer length",
		},
		"unknown-magic": {
			setup: func(t *testing.T) string {
				return buildFakeParquet(t, []byte("footer"), "XYZW")
			},
			errMsg: "not a parquet file",
		},
		"pare-invalid-thrift": {
			setup: func(t *testing.T) string {
				return buildFakeParquet(t, []byte{0xFF}, "PARE")
			},
			errMsg: "parse FileCryptoMetaData",
		},
		"par1-invalid-thrift": {
			setup: func(t *testing.T) string {
				return buildFakeParquet(t, []byte{0xFF}, "PAR1")
			},
			errMsg: "parse FileMetaData",
		},
		// PARE file with valid thrift but no key_metadata → nil
		"pare-no-key-metadata": {
			setup: func(t *testing.T) string {
				fc := parquet.NewFileCryptoMetaData()
				fc.EncryptionAlgorithm = &parquet.EncryptionAlgorithm{AES_GCM_V1: parquet.NewAesGcmV1()}
				buf := thrift.NewTMemoryBuffer()
				proto := thrift.NewTCompactProtocolConf(buf, &thrift.TConfiguration{})
				require.NoError(t, fc.Write(context.Background(), proto))
				return buildFakeParquet(t, buf.Bytes(), "PARE")
			},
			wantNil: true,
		},
		// PAR1 file with encryption_algorithm set but no key hints in footer or columns → nil
		"par1-encrypted-no-hints": {
			setup: func(t *testing.T) string {
				footer := parquet.NewFileMetaData()
				footer.Version = 2
				footer.EncryptionAlgorithm = &parquet.EncryptionAlgorithm{AES_GCM_V1: parquet.NewAesGcmV1()}
				buf := thrift.NewTMemoryBuffer()
				proto := thrift.NewTCompactProtocolConf(buf, &thrift.TConfiguration{})
				require.NoError(t, footer.Write(context.Background(), proto))
				return buildFakeParquet(t, buf.Bytes(), "PAR1")
			},
			wantNil: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			uri := tc.uri
			if tc.setup != nil {
				uri = tc.setup(t)
			}
			hints, err := ReadEncryptionKeyHints(uri, ReadOption{})
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			if tc.wantNil {
				require.Nil(t, hints)
				return
			}
			require.NotNil(t, hints)
			require.Equal(t, tc.wantFooterKM, hints.FooterKeyMetadata)
			require.Len(t, hints.Columns, tc.wantColumnCount)
			if tc.wantColumnPath != "" {
				require.Equal(t, []string{tc.wantColumnPath}, hints.Columns[0].PathInSchema)
				require.Equal(t, "COLUMN_KEY", hints.Columns[0].EncryptionMode)
				require.Equal(t, tc.wantColumnKM, hints.Columns[0].KeyMetadata)
			}
		})
	}
}
