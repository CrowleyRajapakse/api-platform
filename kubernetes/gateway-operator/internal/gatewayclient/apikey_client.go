/*
 *  Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package gatewayclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// APIKeyExpiry mirrors APIKeyCreationRequest.expiresIn.
type APIKeyExpiry struct {
	Duration int64  `json:"duration"`
	Unit     string `json:"unit"`
}

// APIKeyCreatePayload mirrors the management-API APIKeyCreationRequest body
// expected at POST/PUT /<parent>/{parentName}/api-keys[/{name}].
type APIKeyCreatePayload struct {
	Name          string        `json:"name,omitempty"`
	DisplayName   string        `json:"displayName,omitempty"`
	ApiKey        string        `json:"apiKey,omitempty"`
	MaskedApiKey  string        `json:"maskedApiKey,omitempty"`
	ExpiresIn     *APIKeyExpiry `json:"expiresIn,omitempty"`
	ExpiresAt     *time.Time    `json:"expiresAt,omitempty"`
	Issuer        string        `json:"issuer,omitempty"`
	ExternalRefId string        `json:"externalRefId,omitempty"`
}

// BuildAPIKeysPath returns the management-API URL fragment for an API key
// resource nested under its parent. When keyHandle is empty the returned
// path targets the collection (POST), otherwise it targets a single key.
func BuildAPIKeysPath(parentKind, parentHandle, keyHandle string) (string, error) {
	parentBase, err := apiKeyParentPath(parentKind)
	if err != nil {
		return "", err
	}
	if parentHandle == "" {
		return "", fmt.Errorf("apikey parent handle is required")
	}
	if keyHandle == "" {
		return fmt.Sprintf("%s/%s/api-keys", parentBase, url.PathEscape(parentHandle)), nil
	}
	return fmt.Sprintf("%s/%s/api-keys/%s", parentBase, url.PathEscape(parentHandle), url.PathEscape(keyHandle)), nil
}

// apiKeyResourcePathForExists returns the path used by ResourceExists/
// DeployResource where the trailing handle is appended automatically.
func apiKeyResourcePathForExists(parentKind, parentHandle string) (string, error) {
	parentBase, err := apiKeyParentPath(parentKind)
	if err != nil {
		return "", err
	}
	if parentHandle == "" {
		return "", fmt.Errorf("apikey parent handle is required")
	}
	return fmt.Sprintf("%s/%s/api-keys", parentBase, url.PathEscape(parentHandle)), nil
}

// APIKeyExists reports whether the named API key exists under the given
// parent.
func APIKeyExists(ctx context.Context, gatewayEndpoint, parentKind, parentHandle, keyHandle string, auth AuthHeaderFunc) (bool, error) {
	rp, err := apiKeyResourcePathForExists(parentKind, parentHandle)
	if err != nil {
		return false, err
	}
	return ResourceExists(ctx, gatewayEndpoint, rp, keyHandle, auth)
}

// DeployAPIKey POSTs (when exists is false) or PUTs (when exists is true)
// the JSON payload for an API key under the configured parent.
func DeployAPIKey(ctx context.Context, gatewayEndpoint, parentKind, parentHandle, keyHandle string, payload APIKeyCreatePayload, exists bool, auth AuthHeaderFunc) error {
	rp, err := apiKeyResourcePathForExists(parentKind, parentHandle)
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal apikey payload: %w", err)
	}
	return DeployResource(ctx, gatewayEndpoint, rp, keyHandle, body, exists, PayloadContentTypeJSON, auth)
}

// DeleteAPIKey removes the named API key under the given parent.
func DeleteAPIKey(ctx context.Context, gatewayEndpoint, parentKind, parentHandle, keyHandle string, auth AuthHeaderFunc) error {
	rp, err := apiKeyResourcePathForExists(parentKind, parentHandle)
	if err != nil {
		return err
	}
	return DeleteResource(ctx, gatewayEndpoint, rp, keyHandle, auth)
}
