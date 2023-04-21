# witan.gias

gov.uk GIAS (Get Information About Schools) data (downloaded from [get-information-schools.service.gov.uk](https://www.get-information-schools.service.gov.uk/))

See the [GIAS glossary](https://www.get-information-schools.service.gov.uk/glossary) for explanations of terms.

# Usage

``` clojure
(require '[witan.gias.all-data :as gias])
@gias/establishments ; dataset of GIAS all establishment data `edubaseall`
```

# License

GIAS data available under [UK Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

Code copyright © 2021 Mastodon C Ltd, distributed under Eclipse Public License 2.0 as described in LICENSE file.

