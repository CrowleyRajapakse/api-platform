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
	"time"
)

// SubscriptionPlanCreatePayload mirrors the management-API
// SubscriptionPlanCreateRequest. Pointer fields are omitted when nil so the
// gateway-controller fills in defaults.
type SubscriptionPlanCreatePayload struct {
	PlanName           string     `json:"planName"`
	BillingPlan        *string    `json:"billingPlan,omitempty"`
	ExpiryTime         *time.Time `json:"expiryTime,omitempty"`
	Status             *string    `json:"status,omitempty"`
	StopOnQuotaReach   *bool      `json:"stopOnQuotaReach,omitempty"`
	ThrottleLimitCount *int64     `json:"throttleLimitCount,omitempty"`
	ThrottleLimitUnit  *string    `json:"throttleLimitUnit,omitempty"`
}

// SubscriptionPlanUpdatePayload mirrors SubscriptionPlanUpdateRequest.
type SubscriptionPlanUpdatePayload struct {
	PlanName           *string    `json:"planName,omitempty"`
	BillingPlan        *string    `json:"billingPlan,omitempty"`
	ExpiryTime         *time.Time `json:"expiryTime,omitempty"`
	Status             *string    `json:"status,omitempty"`
	StopOnQuotaReach   *bool      `json:"stopOnQuotaReach,omitempty"`
	ThrottleLimitCount *int64     `json:"throttleLimitCount,omitempty"`
	ThrottleLimitUnit  *string    `json:"throttleLimitUnit,omitempty"`
}

// SubscriptionPlanResponse captures the gateway-issued fields returned from
// POST/PUT /subscription-plans.
type SubscriptionPlanResponse struct {
	Id        string `json:"id"`
	PlanName  string `json:"planName"`
	GatewayId string `json:"gatewayId"`
}

// CreateSubscriptionPlan POSTs a SubscriptionPlanCreateRequest payload and
// returns the parsed response (in particular the gateway-issued id).
func CreateSubscriptionPlan(ctx context.Context, gatewayEndpoint string, payload SubscriptionPlanCreatePayload, auth AuthHeaderFunc) (*SubscriptionPlanResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription plan: %w", err)
	}
	respBody, err := CreateResource(ctx, gatewayEndpoint, subscriptionPlansPath, body, PayloadContentTypeJSON, auth)
	if err != nil {
		return nil, err
	}
	var out SubscriptionPlanResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decode subscription plan response: %w", err)
	}
	return &out, nil
}

// UpdateSubscriptionPlan PUTs a SubscriptionPlanUpdateRequest payload to
// /subscription-plans/{planId}.
func UpdateSubscriptionPlan(ctx context.Context, gatewayEndpoint, planID string, payload SubscriptionPlanUpdatePayload, auth AuthHeaderFunc) (*SubscriptionPlanResponse, error) {
	if planID == "" {
		return nil, fmt.Errorf("subscription plan id is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription plan update: %w", err)
	}
	respBody, err := UpdateResource(ctx, gatewayEndpoint, subscriptionPlansPath, planID, body, PayloadContentTypeJSON, auth)
	if err != nil {
		return nil, err
	}
	var out SubscriptionPlanResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decode subscription plan response: %w", err)
	}
	return &out, nil
}

// DeleteSubscriptionPlan DELETEs /subscription-plans/{planId}.
func DeleteSubscriptionPlan(ctx context.Context, gatewayEndpoint, planID string, auth AuthHeaderFunc) error {
	if planID == "" {
		return fmt.Errorf("subscription plan id is required")
	}
	return DeleteResource(ctx, gatewayEndpoint, subscriptionPlansPath, planID, auth)
}
