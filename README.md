# witan.gias

gov.uk GIAS (Get Information About Schools) data (downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/))

See the [GIAS glossary](https://www.get-information-schools.service.gov.uk/glossary) for explanations of terms.

# Usage

``` clojure
(require '[witan.gias :as gias])
gias/default-resource-file-name ; Defalt `edubaseall` resource file of establishment data.
gias/csv-col-names              ; Sorted set of `edubaseall` CSV column names.
gias/csv-col-name->label        ; Map `edubaseall` CSV column name to descriptive label.
gias/csv-col-name->col-name     ; Map `edubaseall` CSV column name to (keyword) column name.
gias/col-names                  ; Sorted set of (keyword) column names.
gias/col-name->csv-col-name     ; Map (keyword) column name to `edubaseall` CSV column name.
gias/col-name->label            ; Map (keyword) column name to descriptive label.
gias/csv-parser-fn              ; parser-fn map for reading `edubaseall` CSV file with CSV column names.
gias/key-fn                     ; Default key-fn to be applied to `edubaseall` CSV column names.
gias/parser-fn                  ; parser-fn map for reading  `edubaseall` CSV file after column names mapped using `key-fn`.

;; Read default `edubaseall` CSV file, with keyword column names.
(gias/->ds)

;; Read a different `edubaseall` resource file
(gias/->ds {::gias/resource-file-name "edubasealldata20230817.csv"})

;; Read an `edubaseall` file from specified path
(gias/->ds {::gias/file-path "/tmp/edubasealldata20230421.csv"})

;; Read selected columns from default `edubaseall`, specified by column name:
(gias/->ds {:column-whitelist (map gias/col-name->csv-col-name [:urn :establishment-name])})

;; Read "URN" as `:int32` (rather than `:string`):
(gias/->ds {:column-whitelist (map gias/col-name->csv-col-name [:urn :establishment-name])
            :parser-fn (assoc gias/parser-fn :urn :int32)})

;; Read seleced columns from default `edubaseall` CSV file with CSV colum names:
(gias/->ds {:column-whitelist ["URN" "EstablishmentName"]
            :key-fn           identity
            :parser-fn        gias/csv-parser-fn})

;; Read selected columns from default `edubaseall` with some custom column names:
(let [key-fn #((merge gias/csv-col-name->col-name {"EstablishmentName" :gias-establishment-name}) % %)]
  (gias/->ds {:column-whitelist ["URN" "EstablishmentName"]
              :key-fn           key-fn
              :parser-fn        (update-keys gias/csv-parser-fn key-fn)}))


```

# License

GIAS data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2021 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.

