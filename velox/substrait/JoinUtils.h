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

#pragma once

#include "velox/core/PlanNode.h"
#include "velox/substrait/proto/substrait/algebra.pb.h"

namespace facebook::velox::substrait {

enum SubstraitJoinKind {
  k_logicalJoin,
  k_physicalHashJoin,
  k_physicalMergeJoin,
};

namespace join {
/// convert velox join type to substrait protocol join type
::substrait::JoinRel_JoinType toProto(core::JoinType joinType);

/// convert substrait join type to velox join type
core::JoinType fromProto(::substrait::JoinRel_JoinType joinType);
} // namespace join

namespace hashJoin {
/// convert velox join type to substrait protocol join type
::substrait::HashJoinRel_JoinType toProto(core::JoinType joinType);

/// convert substrait join type to velox join type
core::JoinType fromProto(::substrait::HashJoinRel_JoinType joinType);
} // namespace hashJoin

namespace mergeJoin {
/// convert velox join type to substrait protocol join type
::substrait::MergeJoinRel_JoinType toProto(core::JoinType joinType);

/// convert substrait join type to velox join type
core::JoinType fromProto(::substrait::MergeJoinRel_JoinType joinType);
} // namespace mergeJoin

} // namespace facebook::velox::substrait
