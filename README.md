# witan.gias

gov.uk GIAS (Get Information About Schools) data (downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/))

See the [GIAS glossary](https://www.get-information-schools.service.gov.uk/glossary) for explanations of terms.

# Usage

``` clojure
(require '[witan.gias.all-data :as gias])
gias/data-file-name ; Name of file containing establishment data.
gias/col-names ; Sorted set of keywork column names for the GIAS establishment data.
gias/col-name->csv-label ; Map keyword column names to the names used in the GIAS CSV file.
gias/col-name->label ; Map keyword column names to descriptive labels for display.
gias/->ds ; Dataset of GIAS all establishment data `edubaseall`.
gias/key-col-names-for-send ; Sorted set of key GIAS columns for SEND work.
gias/key-cols-for-send->ds ; Dataset of key GIAS columns for SEND for all establishments.

```

# License

GIAS data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2021 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.

