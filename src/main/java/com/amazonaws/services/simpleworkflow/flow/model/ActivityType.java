/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder(toBuilder = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityType {

    private String name;

    private String version;

    public static ActivityType fromSdkType(
        software.amazon.awssdk.services.swf.model.ActivityType activityType) {
        return activityType == null ? null
            : ActivityType.builder().name(activityType.name()).version(activityType.version()).build();
    }

    public software.amazon.awssdk.services.swf.model.ActivityType toSdkType() {
        return software.amazon.awssdk.services.swf.model.ActivityType.builder().name(this.name)
                        .version(this.version).build();
    }
}
