// Copyright 2026 The Bucketeer Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"

	proto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
)

// request fingerprint operation prefixes. They are part of the hash input, so
// changes of different types never share a fingerprint even with equal fields.
const (
	fingerprintCreate = "C"
	fingerprintUpdate = "U"
	fingerprintDelete = "D"
)

// canonicalRequestHash computes the stable fingerprint of a request. Fields are
// separated by NUL bytes so unequal inputs can never produce equal encodings.
func canonicalRequestHash(operation string, fields ...string) string {
	h := sha256.New()
	h.Write([]byte(operation))
	for _, f := range fields {
		h.Write([]byte{0})
		h.Write([]byte(f))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sortedAliases joins the aliases in sorted order, so request order does not
// affect the fingerprint.
func sortedAliases(aliases []string) string {
	a := append([]string(nil), aliases...)
	sort.Strings(a)
	return strings.Join(a, "\n")
}

func createRequestFingerprint(req *proto.CreateCodeReferenceRequest) string {
	return canonicalRequestHash(fingerprintCreate,
		req.EnvironmentId,
		req.FeatureId,
		req.FilePath,
		req.FileExtension,
		strconv.Itoa(int(req.LineNumber)),
		req.CodeSnippet,
		req.ContentHash,
		sortedAliases(req.Aliases),
		req.RepositoryName,
		req.RepositoryOwner,
		strconv.Itoa(int(req.RepositoryType)),
		req.RepositoryBranch,
		req.CommitHash,
	)
}

func updateRequestFingerprint(req *proto.UpdateCodeReferenceRequest) string {
	return canonicalRequestHash(fingerprintUpdate,
		req.Id,
		req.EnvironmentId,
		req.FilePath,
		req.FileExtension,
		strconv.Itoa(int(req.LineNumber)),
		req.CodeSnippet,
		req.ContentHash,
		sortedAliases(req.Aliases),
		req.RepositoryName,
		req.RepositoryOwner,
		strconv.Itoa(int(req.RepositoryType)),
		req.RepositoryBranch,
		req.CommitHash,
	)
}

func deleteRequestFingerprint(req *proto.DeleteCodeReferenceRequest) string {
	return canonicalRequestHash(fingerprintDelete,
		req.Id,
		req.EnvironmentId,
	)
}
