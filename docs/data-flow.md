# Media Pipeline Data Flow

Mermaid flowcharts showing conditions that change `pipeline_status`, `rejection_status`, or `error_status`.

---

## Status Key

```mermaid
flowchart TD
    subgraph pipeline_status
        P1([ingested])
        P2([parsed])
        P3([file_accepted])
        P4([metadata_collected])
        P5([media_accepted])
        P6([downloading])
        P7([downloaded])
        P8([transferred])
        P9([complete])
        P10([rejected])
    end

    subgraph rejection_status
        R1{{unfiltered}}
        R2{{accepted}}
        R3{{rejected}}
        R4{{override}}
    end

    subgraph error_status
        E1[[false]]
        E2[[true]]
    end

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class P1,P2,P3,P4,P5,P6,P7,P8,P9,P10 pipeline
    class R1 r_unfiltered
    class R2 r_accepted
    class R3 r_rejected
    class R4 r_override
    class E1 e_false
    class E2 e_true
```

---

## 01 - RSS Ingest

```mermaid
flowchart TD
    START([RSS Entry from Feed]) --> EXISTS{Hash exists<br/>in database?}

    EXISTS -->|Yes| DROPPED([Dropped - No insert])

    EXISTS -->|No| INSERT[Insert to database]

    INSERT --> PIPELINE([pipeline_status = 'ingested'])
    INSERT --> REJECTION{{rejection_status = 'unfiltered'}}
    INSERT --> ERROR[[error_status = false]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20

    class PIPELINE pipeline
    class REJECTION r_unfiltered
    class ERROR e_false
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| New hash | `pipeline_status` | `ingested` |
| New hash | `rejection_status` | `unfiltered` |
| New hash | `error_status` | `false` |

---

## 02 - Collect

Collects ad-hoc items added directly to Transmission (not from RSS feeds). This is a **parallel entry point** to the pipeline. Also re-enables previously rejected items still present in Transmission.

```mermaid
flowchart TB
    subgraph PathA [Path A: From 01 RSS Ingest]
        direction TB
        START_P([pipeline_status = 'ingested'])
        START_R{{rejection_status = 'unfiltered'}}
        START_E[[error_status = false]]

        START_P --> CHECK_NAME{hash == name?}
        START_R --> CHECK_NAME
        START_E --> CHECK_NAME

        CHECK_NAME -->|Yes| SKIP([Skipped - not ready])

        CHECK_NAME -->|No| CHECK_TYPE{media_type == unknown?}

        CHECK_TYPE --> YES[Yes]
        CHECK_TYPE --> NO[No]

        YES --> PIPELINE_A([pipeline_status = 'ingested'])
        YES --> REJECTION_A{{rejection_status = 'override'}}
        YES --> ERROR_A[[error_status = true]]

        NO --> PIPELINE_B([pipeline_status = 'ingested'])
        NO --> REJECTION_B{{rejection_status = 'override'}}
        NO --> ERROR_B[[error_status = false]]
    end

    subgraph Trans [Transmission]
        direction TB
        T2{{rejection_status = 'rejected'}}
    end

    subgraph PathB [Path B: Re-enable Rejected]
        direction TB
        REJECTION_C{{rejection_status = 'override'}}
    end

    T2 --> REJECTION_C

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_A,PIPELINE_B pipeline
    class START_R r_unfiltered
    class T2 r_rejected
    class REJECTION_A,REJECTION_B,REJECTION_C r_override
    class START_E,ERROR_B e_false
    class ERROR_A e_true
```

### Summary

| Path | Condition | Status | Value |
|------|-----------|--------|-------|
| A | New ad-hoc item | `pipeline_status` | `ingested` |
| A | New ad-hoc item | `rejection_status` | `override` |
| A | media_type == unknown | `error_status` | `true` |
| A | media_type == unknown | `error_condition` | `'media_type is unknown'` |
| A | media_type != unknown | `error_status` | `false` |
| B | Previously rejected item in Transmission | `rejection_status` | `override` |

---

## 03 - Parse

Parses media titles to extract metadata (resolution, codec, year, season, episode, etc.) and validates mandatory fields based on media type.

```mermaid
flowchart TB
    subgraph Input [Input: From 01/02]
        direction TB
        START_P([pipeline_status = 'ingested'])
        START_R1{{rejection_status = 'unfiltered'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    START_P --> VALIDATE{Validation passes?}
    START_R1 --> VALIDATE
    START_R2 --> VALIDATE
    START_E --> VALIDATE

    VALIDATE --> YES[Yes]
    VALIDATE --> NO[No]

    YES --> PIPELINE_A([pipeline_status = 'parsed'])

    NO --> ERROR_A[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_A pipeline
    class START_R1 r_unfiltered
    class START_R2 r_override
    class START_E e_false
    class ERROR_A e_true
```

### Validation Rules

| Media Type | Mandatory Fields |
|------------|------------------|
| All | `media_title` |
| movie | `release_year` |
| tv_show | `season`, `episode` |
| tv_season | `season` |

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| Validation passes | `pipeline_status` | `parsed` |
| Validation fails | `error_status` | `true` |
| Validation fails | `error_condition` | e.g. `'media_title is null'` |

---

## 04 - File Filtration

Filters media based on file metadata (resolution, codec, etc.) using rules from `filter-parameters.yaml`. Override status bypasses filtering.

```mermaid
flowchart TB
    subgraph Input [Input: From 03]
        direction TB
        START_P([pipeline_status = 'parsed'])
        START_R1{{rejection_status = 'unfiltered'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    START_P --> CHECK_OVERRIDE{rejection_status == override?}
    START_R1 --> CHECK_OVERRIDE
    START_R2 --> CHECK_OVERRIDE
    START_E --> CHECK_OVERRIDE

    CHECK_OVERRIDE --> YES_OVERRIDE[Yes]
    CHECK_OVERRIDE --> NO_OVERRIDE[No]

    YES_OVERRIDE --> PIPELINE_OVERRIDE([pipeline_status = 'file_accepted'])
    YES_OVERRIDE --> REJECTION_OVERRIDE{{rejection_status = 'override'}}

    NO_OVERRIDE --> FILTER{Filter passes?}

    FILTER --> YES_FILTER[Yes]
    FILTER --> NO_FILTER[No]

    YES_FILTER --> PIPELINE_ACCEPT([pipeline_status = 'file_accepted'])
    YES_FILTER --> REJECTION_ACCEPT{{rejection_status = 'accepted'}}

    NO_FILTER --> PIPELINE_REJECT([pipeline_status = 'rejected'])
    NO_FILTER --> REJECTION_REJECT{{rejection_status = 'rejected'}}

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20

    class START_P,PIPELINE_OVERRIDE,PIPELINE_ACCEPT,PIPELINE_REJECT pipeline
    class START_R1 r_unfiltered
    class START_R2,REJECTION_OVERRIDE r_override
    class REJECTION_ACCEPT r_accepted
    class REJECTION_REJECT r_rejected
    class START_E e_false
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| rejection_status == override | `pipeline_status` | `file_accepted` |
| rejection_status == override | `rejection_status` | `override` (unchanged) |
| Filter passes | `pipeline_status` | `file_accepted` |
| Filter passes | `rejection_status` | `accepted` |
| Filter fails | `pipeline_status` | `rejected` |
| Filter fails | `rejection_status` | `rejected` |
| Exception during filter | `error_status` | `true` |

---

## 05 - Metadata Collection

Collects metadata from TMDB (search + details) and OMDb (ratings). Items with existing metadata in DB skip API calls.

```mermaid
flowchart TB
    subgraph Input [Input: From 04]
        direction TB
        START_P([pipeline_status = 'file_accepted'])
        START_R1{{rejection_status = 'accepted'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    START_P --> SEARCH{TMDB search finds result?}
    START_R1 --> SEARCH
    START_R2 --> SEARCH
    START_E --> SEARCH

    SEARCH --> YES_SEARCH[Yes]
    SEARCH --> NO_SEARCH[No]

    NO_SEARCH --> PIPELINE_REJECT([pipeline_status = 'rejected'])
    NO_SEARCH --> REJECTION_REJECT{{rejection_status = 'rejected'}}

    YES_SEARCH --> DETAILS{Details API succeeds?}

    DETAILS --> YES_DETAILS[Yes]
    DETAILS --> NO_DETAILS[No]

    NO_DETAILS --> ERROR_API[[error_status = true]]

    YES_DETAILS --> PIPELINE_SUCCESS([pipeline_status = 'metadata_collected'])

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_REJECT,PIPELINE_SUCCESS pipeline
    class START_R1 r_accepted
    class START_R2 r_override
    class REJECTION_REJECT r_rejected
    class START_E e_false
    class ERROR_API e_true
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| TMDB search fails | `pipeline_status` | `rejected` |
| TMDB search fails | `rejection_status` | `rejected` |
| API error | `error_status` | `true` |
| Metadata collected | `pipeline_status` | `metadata_collected` |

---

## 06 - Media Filtration

ML-powered filtering using reel-driver. TV shows/seasons are exempt. Movies are scored and compared against threshold.

```mermaid
flowchart TB
    subgraph Input [Input: From 05]
        direction TB
        START_P([pipeline_status = 'metadata_collected'])
        START_R1{{rejection_status = 'accepted'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    START_P --> CHECK_TYPE{media_type?}
    START_R1 --> CHECK_TYPE
    START_R2 --> CHECK_TYPE
    START_E --> CHECK_TYPE

    CHECK_TYPE --> TV[tv_show / tv_season]
    CHECK_TYPE --> MOVIE[movie]

    TV --> PIPELINE_TV([pipeline_status = 'media_accepted'])
    TV --> REJECTION_TV{{rejection_status = 'accepted'}}

    MOVIE --> CHECK_OVERRIDE{rejection_status == override?}

    CHECK_OVERRIDE --> YES_OVERRIDE[Yes]
    CHECK_OVERRIDE --> NO_OVERRIDE[No]

    YES_OVERRIDE --> PIPELINE_OVERRIDE([pipeline_status = 'media_accepted'])
    YES_OVERRIDE --> REJECTION_OVERRIDE{{rejection_status = 'override'}}

    NO_OVERRIDE --> CHECK_IMDB{Has imdb_id?}

    CHECK_IMDB --> NO_IMDB[No]
    CHECK_IMDB --> YES_IMDB[Yes]

    NO_IMDB --> PIPELINE_NO_IMDB([pipeline_status = 'rejected'])
    NO_IMDB --> REJECTION_NO_IMDB{{rejection_status = 'rejected'}}

    YES_IMDB --> PREDICT{reel-driver prediction}

    PREDICT --> ABOVE[probability >= threshold]
    PREDICT --> BELOW[probability < threshold]
    PREDICT --> API_ERROR[API error]

    ABOVE --> PIPELINE_ACCEPT([pipeline_status = 'media_accepted'])
    ABOVE --> REJECTION_ACCEPT{{rejection_status = 'accepted'}}

    BELOW --> PIPELINE_REJECT([pipeline_status = 'rejected'])
    BELOW --> REJECTION_REJECT{{rejection_status = 'rejected'}}

    API_ERROR --> ERROR_TRUE[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_TV,PIPELINE_OVERRIDE,PIPELINE_NO_IMDB,PIPELINE_ACCEPT,PIPELINE_REJECT pipeline
    class START_R1,REJECTION_TV,REJECTION_ACCEPT r_accepted
    class START_R2,REJECTION_OVERRIDE r_override
    class REJECTION_NO_IMDB,REJECTION_REJECT r_rejected
    class START_E e_false
    class ERROR_TRUE e_true
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| TV show / TV season | `pipeline_status` | `media_accepted` |
| TV show / TV season | `rejection_status` | `accepted` |
| rejection_status == override | `pipeline_status` | `media_accepted` |
| rejection_status == override | `rejection_status` | `override` (unchanged) |
| No imdb_id | `pipeline_status` | `rejected` |
| No imdb_id | `rejection_status` | `rejected` |
| probability >= threshold | `pipeline_status` | `media_accepted` |
| probability >= threshold | `rejection_status` | `accepted` |
| probability < threshold | `pipeline_status` | `rejected` |
| probability < threshold | `rejection_status` | `rejected` |
| reel-driver API error | `error_status` | `true` |

---

## 07 - Initiation

Adds accepted media items to Transmission to begin downloading.

```mermaid
flowchart TB
    subgraph Input [Input: From 06]
        direction TB
        START_P([pipeline_status = 'media_accepted'])
        START_R1{{rejection_status = 'accepted'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    subgraph Trans [Transmission]
        direction TB
        ADD[Add torrent by hash]
    end

    START_P --> ADD
    START_R1 --> ADD
    START_R2 --> ADD
    START_E --> ADD

    ADD --> SUCCESS[Success]
    ADD --> FAILURE[Failure]

    SUCCESS --> PIPELINE_DL([pipeline_status = 'downloading'])

    FAILURE --> ERROR_TRUE[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_DL pipeline
    class START_R1 r_accepted
    class START_R2 r_override
    class START_E e_false
    class ERROR_TRUE e_true
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| Torrent added to Transmission | `pipeline_status` | `downloading` |
| Failed to add torrent | `error_status` | `true` |

---

## 08 - Download Check

Monitors Transmission for download progress. Handles missing items by re-ingesting.

```mermaid
flowchart TB
    subgraph Input [Input: From 07]
        direction TB
        START_P([pipeline_status = 'downloading'])
        START_R1{{rejection_status = 'accepted'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    subgraph Trans [Transmission]
        direction TB
        CHECK[Check torrent status]
    end

    START_P --> CHECK
    START_R1 --> CHECK
    START_R2 --> CHECK
    START_E --> CHECK

    CHECK --> NOT_FOUND[Not found in Transmission]
    CHECK --> IN_PROGRESS[Still downloading]
    CHECK --> COMPLETE[Download complete]

    NOT_FOUND --> PIPELINE_REINGEST([pipeline_status = 'ingested'])
    NOT_FOUND --> REJECTION_REINGEST{{rejection_status = 'unfiltered'}}
    NOT_FOUND --> ERROR_REINGEST[[error_status = false]]

    IN_PROGRESS --> NO_CHANGE[No status change]

    COMPLETE --> FILENAME{Valid filename?}

    FILENAME --> YES_FILE[Yes]
    FILENAME --> NO_FILE[No]

    YES_FILE --> PIPELINE_DL([pipeline_status = 'downloaded'])

    NO_FILE --> ERROR_FILE[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef r_unfiltered fill:#e0e0e0,stroke:#757575,stroke-width:2px,color:#212121
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_REINGEST,PIPELINE_DL pipeline
    class START_R1 r_accepted
    class START_R2 r_override
    class REJECTION_REINGEST r_unfiltered
    class START_E,ERROR_REINGEST e_false
    class ERROR_FILE e_true
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| Not found in Transmission | `pipeline_status` | `ingested` (re-ingest) |
| Not found in Transmission | `rejection_status` | `unfiltered` |
| Not found in Transmission | `error_status` | `false` (reset) |
| Still downloading | - | No change |
| Download complete + valid filename | `pipeline_status` | `downloaded` |
| Download complete + invalid filename | `error_status` | `true` |

---

## 09 - Transfer

Moves downloaded files from download directory to Plex library based on media type.

```mermaid
flowchart TB
    subgraph Input [Input: From 08]
        direction TB
        START_P([pipeline_status = 'downloaded'])
        START_R1{{rejection_status = 'accepted'}}
        START_R2{{rejection_status = 'override'}}
        START_E[[error_status = false]]
    end

    START_P --> GEN_PATH{Generate paths succeeds?}
    START_R1 --> GEN_PATH
    START_R2 --> GEN_PATH
    START_E --> GEN_PATH

    GEN_PATH --> YES_PATH[Yes]
    GEN_PATH --> NO_PATH[No]

    NO_PATH --> ERROR_PATH[[error_status = true]]

    YES_PATH --> TRANSFER{File transfer succeeds?}

    TRANSFER --> YES_TRANSFER[Yes]
    TRANSFER --> NO_TRANSFER[No]

    YES_TRANSFER --> PIPELINE_TRANS([pipeline_status = 'transferred'])

    NO_TRANSFER --> ERROR_TRANS[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P,PIPELINE_TRANS pipeline
    class START_R1 r_accepted
    class START_R2 r_override
    class START_E e_false
    class ERROR_PATH,ERROR_TRANS e_true
```

### Summary

| Condition | Status | Value |
|-----------|--------|-------|
| Path generation fails | `error_status` | `true` |
| File transfer fails | `error_status` | `true` |
| File transfer succeeds | `pipeline_status` | `transferred` |

---

## 10 - Cleanup

Removes torrents from Transmission after successful transfer or when items exceed time limits.

```mermaid
flowchart TB
    subgraph InputA [Path A: Transferred Items]
        direction TB
        START_P_A([pipeline_status = 'transferred'])
        START_R1_A{{rejection_status = 'accepted'}}
        START_R2_A{{rejection_status = 'override'}}
        START_E_A[[error_status = false]]
    end

    subgraph InputB [Path B: Hung Items]
        direction TB
        START_ANY([Any pipeline_status in Transmission])
        START_TIME[Exceeded time limit]
    end

    subgraph Trans [Transmission]
        direction TB
        REMOVE_A[Remove torrent]
        REMOVE_B[Remove torrent]
    end

    START_P_A --> CHECK_TIME{Time since transfer > delay?}
    START_R1_A --> CHECK_TIME
    START_R2_A --> CHECK_TIME
    START_E_A --> CHECK_TIME

    CHECK_TIME --> YES_TIME[Yes]
    CHECK_TIME --> NO_TIME[No]

    NO_TIME --> NO_CHANGE_A[No status change]

    YES_TIME --> REMOVE_A

    REMOVE_A --> SUCCESS_A[Success]
    REMOVE_A --> FAILURE_A[Failure]

    SUCCESS_A --> PIPELINE_COMPLETE([pipeline_status = 'complete'])

    FAILURE_A --> ERROR_A[[error_status = true]]

    START_ANY --> START_TIME
    START_TIME --> REMOVE_B

    REMOVE_B --> SUCCESS_B[Success]
    REMOVE_B --> FAILURE_B[Failure]

    SUCCESS_B --> PIPELINE_REJECT([pipeline_status = 'rejected'])
    SUCCESS_B --> REJECTION_REJECT{{rejection_status = 'rejected'}}

    FAILURE_B --> ERROR_B[[error_status = true]]

    classDef pipeline fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef r_accepted fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef r_override fill:#e1bee7,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef r_rejected fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef e_false fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef e_true fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#b71c1c

    class START_P_A,PIPELINE_COMPLETE,PIPELINE_REJECT pipeline
    class START_R1_A r_accepted
    class START_R2_A r_override
    class REJECTION_REJECT r_rejected
    class START_E_A e_false
    class ERROR_A,ERROR_B e_true
```

### Summary

| Path | Condition | Status | Value |
|------|-----------|--------|-------|
| A | Transferred + delay exceeded + removal succeeds | `pipeline_status` | `complete` |
| A | Transferred + delay not exceeded | - | No change |
| A | Removal fails | `error_status` | `true` |
| B | Hung item + removal succeeds | `pipeline_status` | `rejected` |
| B | Hung item + removal succeeds | `rejection_status` | `rejected` |
| B | Hung item + removal succeeds | `rejection_reason` | `'exceeded time limit'` |
| B | Removal fails | `error_status` | `true` |
