# witan.gias

gov.uk GIAS (Get Information About Schools) data (downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/))

See the [GIAS glossary](https://www.get-information-schools.service.gov.uk/glossary) for explanations of terms.

# Usage

``` clojure
(require '[witan.gias :as gias])
gias/default-edubaseall-resource-file-name ; Defalt `edubaseall` resource file of establishment data.
gias/edubaseall-csv-columns                ; Map `edubaseall` CSV column names to (maps of) metadata.
gias/edubaseall-columns                    ; Same as csv-columns but keyed by the (keyword) dataset column names.
gias/edubaseall-csv-parser-fn              ; parser-fn map for reading `edubaseall` CSV file with CSV column names.
gias/edubaseall-csv-key-fn                 ; Default key-fn to be applied to `edubaseall` CSV column names.
gias/edubaseall-parser-fn                  ; Default parser-fn map for reading  `edubaseall` CSV file
                                           ; after column names mapped using `edubaseall-csv-key-fn`.

;; Read default `edubaseall` CSV file, with keyword column names.
(gias/edubaseall->ds)

;; Read a different `edubaseall` resource file
(gias/edubaseall->ds {::gias/edubaseall-resource-file-name "edubasealldata20230817.csv"})

;; Read an `edubaseall` file from specified path
(gias/edubaseall->ds {::gias/edubaseall-file-path "/tmp/edubasealldata20230421.csv"})

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

# License

GIAS data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2021 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.

