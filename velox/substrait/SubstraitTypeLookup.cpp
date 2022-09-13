/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/substrait/SubstraitTypeLookup.h"

namespace facebook::velox::substrait {

SubstraitTypeLookup::SubstraitTypeLookup(
    const std::vector<SubstraitTypeAnchorPtr>& types) {
  for (auto& typeAnchor : types) {
    signatures_.insert({typeAnchor->name, typeAnchor});
  }
}

std::optional<SubstraitTypeAnchorPtr> SubstraitTypeLookup::lookupType(
    const std::string& typeName) const {
  if (signatures_.find(typeName) != signatures_.end()) {
    return std::make_optional(signatures_.at(typeName));
  }
  return std::nullopt;
}

std::optional<SubstraitTypeAnchorPtr> SubstraitTypeLookup::lookupUnknownType()
    const {
  return lookupType("unknown");
}

} // namespace facebook::velox::substrait