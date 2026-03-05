# Multi-Table CityJSON Loading Implementation Plan

## Overview

Add a new table function `cityjson_metadata` that returns a single row with dataset-level metadata from a CityJSON file, complementing the existing `read_cityjson` table function which returns city objects.

## Usage

```sql
-- Read CityJSON metadata
SELECT * FROM cityjson_metadata('buildings.city.json');

-- Read CityJSON city objects
SELECT * FROM read_cityjson('buildings.city.json');

-- Create persistent tables (multi-table pattern)
CREATE TABLE metadata AS SELECT * FROM cityjson_metadata('buildings.city.json');
CREATE TABLE city_objects AS SELECT * FROM read_cityjson('buildings.city.json');

-- Query the tables
SELECT version, city_objects_count FROM metadata;
SELECT * FROM city_objects WHERE object_type = 'Building';
```

## Implementation Phases

### Phase 1: Metadata Table Schema ✅

Created `MetadataTableUtils` class with the following:

| Column              | Type             | Source                    |
| ------------------- | ---------------- | ------------------------- |
| id                  | INTEGER          | Always 1                  |
| version             | VARCHAR          | CityJSON.version          |
| identifier          | VARCHAR          | metadata.identifier       |
| title               | VARCHAR          | metadata.title            |
| reference_date      | DATE             | metadata.reference_date   |
| transform_scale     | STRUCT(x,y,z)    | transform.scale           |
| transform_translate | STRUCT(x,y,z)    | transform.translate       |
| geographical_extent | STRUCT(6 fields) | Computed from CityObjects |
| reference_system    | STRUCT           | CRS information           |
| point_of_contact    | STRUCT           | metadata.point_of_contact |
| city_objects_count  | BIGINT           | Count of CityObjects      |

### Phase 2: Table Function Implementation ✅

- Created `cityjson_metadata` table function
- Registered in extension loader alongside `read_cityjson`
- Returns structured DuckDB types including nested STRUCTs

### Phase 3: Testing ✅

Added comprehensive tests in `test/sql/cityjson_metadata.test`:

- Single row return
- Version extraction
- City objects count
- Transform struct fields
- Multi-table pattern compatibility
- Error handling

---

## Progress

| Phase   | Status      | Description                   |
| ------- | ----------- | ----------------------------- |
| Phase 1 | ✅ Complete | Metadata Table Schema         |
| Phase 2 | ✅ Complete | Table Function Implementation |
| Phase 3 | ✅ Complete | Testing                       |
