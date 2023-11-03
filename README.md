# witan.gias

gov.uk GIAS (Get Information About Schools) data (downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/))

See the [GIAS glossary](https://www.get-information-schools.service.gov.uk/glossary) for explanations of terms.

# Usage

## Setup

``` clojure
(require '[witan.gias :as gias])
```

##  Getting basic `edubaseall` datasets

From "all establishments" `edubaseall` CSV files downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/):

```clojure
;; Default `edubaseall` resource file of all establishment data.
gias/default-edubaseall-resource-file-name

;; Read default `edubaseall` CSV file, with keyword column names.
(gias/edubaseall->ds)

;; Read a different `edubaseall` resource file
(gias/edubaseall->ds {::gias/edubaseall-resource-file-name "edubasealldata20230817.csv"})

;; Read an `edubaseall` file from specified path
(gias/edubaseall->ds {::gias/edubaseall-file-path "/tmp/edubasealldata20230421.csv"})
```

### SEN Provision Type strings are parsed

Note that the "SEN Provision Type" strings in CSV columns "SEN1 (name)" to "SEN13 (name)" are parsed to:

- Return abbreviations only, not "ABBREVIATION - NAME" or "Not Applicable" as is in the edubaseall CSV.
- Return abbreviations in upper case, which results in "SpLD" in the edubaseall CSV being returned as "SPLD" (which is the abbreviation used in the SEN2 return).
- Map "Not Applicable" (used in "SEN1 (name)" to missing. 
  (Note however that some schools have SEN provision type 1 entered as "Not Applicable" but still have some of types 2-13 filled in!?)

The mapping is via function `gias/parse-sen-provision-type-name`, implementing map:
```clojure
{"Not Applicable"                                   ::ds/missing ; Note missing
 "SpLD - Specific Learning Difficulty"              "SPLD"       ; Note upper-case
 "MLD - Moderate Learning Difficulty"               "MLD"
 "SLD - Severe Learning Difficulty"                 "SLD"
 "PMLD - Profound and Multiple Learning Difficulty" "PMLD"
 "SEMH - Social, Emotional and Mental Health"       "SEMH"
 "SLCN - Speech, language and Communication"        "SLCN"
 "HI - Hearing Impairment"                          "HI"
 "VI - Visual Impairment"                           "VI"
 "MSI - Multi-Sensory Impairment"                   "MSI"
 "PD - Physical Disability"                         "PD"
 "ASD - Autistic Spectrum Disorder"                 "ASD"
 "OTH - Other Difficulty/Disability"                "OTH"}
```

## GIAS for SEND

Function `edubaseall-send->ds` reads SEND related columns from a GIAS `edubaseall` CSV export :

- `:urn`
- `:last-changed-date`
- `:ukprn`
- `:establishment-number`
- `:establishment-name`
- `:type-of-establishment-name`
- `:establishment-type-group-name`
- `:la-code`
- `:la-name`
- `:establishment-status-name`
- `:open-date`
- `:close-date`
- `:phase-of-education-name`
- `:statutory-low-age`
- `:statutory-high-age`
- `:further-education-type-name`
- `:school-census-date`
- `:school-capacity`
- `:number-of-pupils`
- `:places-pru`
- `:senpru-name`
- `:special-classes-name`
- `:sen-stat`
- `:sen-no-stat`
- `:sen-unit-capacity`
- `:sen-unit-on-roll`
- `:resourced-provision-capacity`
- `:resourced-provision-on-roll`

…with additional derived columns:

   - `:sen-provision-types-vec` - vector of (upper-case) SEN provision type abbreviations extracted from \"SEN1\"-\"SEN13\"
   - `:resourced-provision?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has RP.
   - `:sen-unit?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has a SENU.

The column specification is in `gias/edubaseall-send-columns`.

## Customised reads

The `ns` provides `defs` from which the `:key-fn` & `:parser-fn`  used for the default reads above are derived:

```clojure
; Map `edubaseall` CSV column names to (maps of) metadata.
gias/edubaseall-csv-columns
; Same as csv-columns but keyed by the (keyword) dataset column names.
gias/edubaseall-columns
; parser-fn map for reading `edubaseall` CSV file with CSV column names.
gias/edubaseall-csv-parser-fn
; Default key-fn to be applied to `edubaseall` CSV column names.
gias/edubaseall-csv-key-fn
; Default parser-fn map for reading  `edubaseall` CSV file
gias/edubaseall-parser-fn
```

The `edubaseall->ds` options map is merged into the TMD `->dataset` options map, allowing customisation of the CSV read, for example:

```clojure
;; Read selected columns from default `edubaseall`, specified by CSV column name:
(gias/edubaseall->ds {:column-allowlist ["URN" "EstablishmentName"]})

;; Read "URN" as `:int32` (rather than `:string`):
(gias/edubaseall->ds {:column-allowlist ["URN" "EstablishmentName"]
                      :parser-fn (assoc gias/edubaseall-parser-fn :urn :int32)})

;; Read selected columns from default `edubaseall` CSV file with CSV colum names:
(gias/edubaseall->ds {:column-allowlist ["URN" "EstablishmentName"]
                      :key-fn           identity
                      :parser-fn        gias/edubaseall-csv-parser-fn})

;; Read selected columns from default `edubaseall` with some custom column names:
(let [key-fn #((merge (update-vals gias/csv-columns :col-name) {"EstablishmentName" :gias-establishment-name}) % %)]
  (gias/edubaseall->ds {:column-allowlist ["URN" "EstablishmentName"]
                        :key-fn           key-fn
                        :parser-fn        (update-keys gias/edubaseall-csv-parser-fn key-fn)}))
```

`edubaseall-send->ds` supports the same options map as `edubaseall->ds` but as custom `:column-allowlist`, `:column-blocklist` and `:key-fn` are used to select and process the SEND related columns these options are ignored (over-ridden).

# License

GIAS data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2021 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.

