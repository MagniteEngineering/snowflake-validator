# Snowflake Validation Script

This repo contains a Snowflake python validation script that is executable in Snowflake Worksheets.

This script is to be used to validate views when sharing Audience Data with Magnite via Snowflake.

This script will confirm if the views are the correct schema/format before sharing Audience Data to Magnite.
 

## What does it Validate
This validation script validates -
  - Two View Names are - 
    - `MEMBERSHIP_SECURE_VIEW`
    - `TAXONOMY_SECURE_VIEW`
  - Two Views are set to `SECURE`
  - Two Views have `CHANGE-TRACKING` set to `ON`
  - Two Views will be validated against the following schemas - 
    
### Membership Table Requirements

| Field | Data type | Required/Optional |
| ------ | ------ | ------ |
| StorageId | VARCHAR(255) | Required |
| IdType | VARCHAR(36) | Required |
| SegmentId | VARCHAR(36) | Required |
| Active | BOOLEAN | Required |
| UpdateTimestamp  | TIMESTAMP | Required |

### Taxonomy Table Requirements

| Field | Data type | Required/Optional |
| ------ | ------ | ------ |
| ClientName | VARCHAR(255) | Required |
| AccountId | VARCHAR(36) | Required |
| DataShareSettings | VARCHAR(36) | Required |
| SegmentId | VARCHAR(36) | Required |
| SegmentName | VARCHAR(255) | Required |
| SegmentDescription | VARCHAR(255) | Optional |
| SegmentCPM | DOUBLE | Optional |
| Platform | VARCHAR(255) | Required |
| Active | BOOLEAN | Required | True |
| UpdateTimestamp | TIMESTAMP | Required |


## How to use

Steps to use the script are in the following document -

https://docs.google.com/document/u/1/d/e/2PACX-1vR2GuqGfhCHpXodO6vP-I4CP4YE2y5DXxu552b_OPSKU-0rOI_Znb8WdEabgSw2PncxAIs9QhjoY1-j/pub


