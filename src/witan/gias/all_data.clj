(ns witan.gias.all-data
  "Read in \"Get Information About Schools\" (GIAS) all establishment data from CSV file downloaded from [get-information-schools.service.gov.uk/Downloads](https://www.get-information-schools.service.gov.uk/Downloads)"
  (:require [clojure.java.io :as io]
            [clojure.set     :as set]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]))



;;; # Data files
(def data-file-name
  "Name of file containing establishment data"
  "edubasealldata20230421.csv")



;;; # Column names, labels and types
(def col-name->order
  (let [m (zipmap [:urn
                   :la-code
                   :la-name
                   :establishment-number
                   :establishment-name
                   :type-of-establishment-code
                   :type-of-establishment-name
                   :establishment-type-group-code
                   :establishment-type-group-name
                   :establishment-status-code
                   :establishment-status-name
                   :reason-establishment-opened-code
                   :reason-establishment-opened-name
                   :open-date
                   :reason-establishment-closed-code
                   :reason-establishment-closed-name
                   :close-date
                   :phase-of-education-code
                   :phase-of-education-name
                   :statutory-low-age
                   :statutory-high-age
                   :boarders-code
                   :boarders-name
                   :nursery-provision-name
                   :official-sixth-form-code
                   :official-sixth-form-name
                   :gender-code
                   :gender-name
                   :religious-character-code
                   :religious-character-name
                   :religious-ethos-name
                   :diocese-code
                   :diocese-name
                   :admissions-policy-code
                   :admissions-policy-name
                   :school-capacity
                   :special-classes-code
                   :special-classes-name
                   :census-date
                   :number-of-pupils
                   :number-of-boys
                   :number-of-girls
                   :percentage-fsm
                   :trust-school-flag-code
                   :trust-school-flag-name
                   :trusts-code
                   :trusts-name
                   :school-sponsor-flag-name
                   :school-sponsors-name
                   :federation-flag-name
                   :federations-code
                   :federations-name
                   :ukprn
                   :fehe-identifier
                   :further-education-type-name
                   :ofsted-last-insp
                   :ofsted-special-measures-code
                   :ofsted-special-measures-name
                   :last-changed-date
                   :street
                   :locality
                   :address3
                   :town
                   :county-name
                   :postcode
                   :school-website
                   :telephone-num
                   :head-title-name
                   :head-first-name
                   :head-last-name
                   :head-preferred-job-title
                   :bso-inspectorate-name-name
                   :inspectorate-report
                   :date-of-last-inspection-visit
                   :next-inspection-visit
                   :teen-moth-name
                   :teen-moth-places
                   :ccf-name
                   :senpru-name
                   :ebd-name
                   :places-pru
                   :ft-prov-name
                   :ed-by-other-name
                   :section41-approved-name
                   :sen1-name
                   :sen2-name
                   :sen3-name
                   :sen4-name
                   :sen5-name
                   :sen6-name
                   :sen7-name
                   :sen8-name
                   :sen9-name
                   :sen10-name
                   :sen11-name
                   :sen12-name
                   :sen13-name
                   :type-of-resourced-provision-name
                   :resourced-provision-on-roll
                   :resourced-provision-capacity
                   :sen-unit-on-roll
                   :sen-unit-capacity
                   :gor-code
                   :gor-name
                   :district-administrative-code
                   :district-administrative-name
                   :administrative-ward-code
                   :administrative-ward-name
                   :parliamentary-constituency-code
                   :parliamentary-constituency-name
                   :urban-rural-code
                   :urban-rural-name
                   :gssla-code-name
                   :easting
                   :northing
                   :msoa-name
                   :lsoa-name
                   :inspectorate-name-name
                   :sen-stat
                   :sen-no-stat
                   :boarding-establishment-name
                   :props-name
                   :previous-la-code
                   :previous-la-name
                   :previous-establishment-number
                   :ofsted-rating-name
                   :rsc-region-name
                   :country-name
                   :uprn
                   :site-name
                   :qab-name-code
                   :qab-name-name
                   :establishment-accredited-code
                   :establishment-accredited-name
                   :qab-report
                   :ch-number
                   :msoa-code
                   :lsoa-code
                   :fsm]
                  (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def col-names
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get col-name->order k1) k1]
                              [(get col-name->order k2) k2]))
         (keys col-name->order)))

(def col-name->csv-label
  "Map column name to the label used in the CSV file."
  (into (sorted-map-by (fn [k1 k2] (compare [(get col-name->order k1) k1]
                                            [(get col-name->order k2) k2]))) 
        {:urn                              "URN"
         :la-code                          "LA (code)"
         :la-name                          "LA (name)"
         :establishment-number             "EstablishmentNumber"
         :establishment-name               "EstablishmentName"
         :type-of-establishment-code       "TypeOfEstablishment (code)"
         :type-of-establishment-name       "TypeOfEstablishment (name)"
         :establishment-type-group-code    "EstablishmentTypeGroup (code)"
         :establishment-type-group-name    "EstablishmentTypeGroup (name)"
         :establishment-status-code        "EstablishmentStatus (code)"
         :establishment-status-name        "EstablishmentStatus (name)"
         :reason-establishment-opened-code "ReasonEstablishmentOpened (code)"
         :reason-establishment-opened-name "ReasonEstablishmentOpened (name)"
         :open-date                        "OpenDate"
         :reason-establishment-closed-code "ReasonEstablishmentClosed (code)"
         :reason-establishment-closed-name "ReasonEstablishmentClosed (name)"
         :close-date                       "CloseDate"
         :phase-of-education-code          "PhaseOfEducation (code)"
         :phase-of-education-name          "PhaseOfEducation (name)"
         :statutory-low-age                "StatutoryLowAge"
         :statutory-high-age               "StatutoryHighAge"
         :boarders-code                    "Boarders (code)"
         :boarders-name                    "Boarders (name)"
         :nursery-provision-name           "NurseryProvision (name)"
         :official-sixth-form-code         "OfficialSixthForm (code)"
         :official-sixth-form-name         "OfficialSixthForm (name)"
         :gender-code                      "Gender (code)"
         :gender-name                      "Gender (name)"
         :religious-character-code         "ReligiousCharacter (code)"
         :religious-character-name         "ReligiousCharacter (name)"
         :religious-ethos-name             "ReligiousEthos (name)"
         :diocese-code                     "Diocese (code)"
         :diocese-name                     "Diocese (name)"
         :admissions-policy-code           "AdmissionsPolicy (code)"
         :admissions-policy-name           "AdmissionsPolicy (name)"
         :school-capacity                  "SchoolCapacity"
         :special-classes-code             "SpecialClasses (code)"
         :special-classes-name             "SpecialClasses (name)"
         :census-date                      "CensusDate"
         :number-of-pupils                 "NumberOfPupils"
         :number-of-boys                   "NumberOfBoys"
         :number-of-girls                  "NumberOfGirls"
         :percentage-fsm                   "PercentageFSM"
         :trust-school-flag-code           "TrustSchoolFlag (code)"
         :trust-school-flag-name           "TrustSchoolFlag (name)"
         :trusts-code                      "Trusts (code)"
         :trusts-name                      "Trusts (name)"
         :school-sponsor-flag-name         "SchoolSponsorFlag (name)"
         :school-sponsors-name             "SchoolSponsors (name)"
         :federation-flag-name             "FederationFlag (name)"
         :federations-code                 "Federations (code)"
         :federations-name                 "Federations (name)"
         :ukprn                            "UKPRN"
         :fehe-identifier                  "FEHEIdentifier"
         :further-education-type-name      "FurtherEducationType (name)"
         :ofsted-last-insp                 "OfstedLastInsp"
         :ofsted-special-measures-code     "OfstedSpecialMeasures (code)"
         :ofsted-special-measures-name     "OfstedSpecialMeasures (name)"
         :last-changed-date                "LastChangedDate"
         :street                           "Street"
         :locality                         "Locality"
         :address3                         "Address3"
         :town                             "Town"
         :county-name                      "County (name)"
         :postcode                         "Postcode"
         :school-website                   "SchoolWebsite"
         :telephone-num                    "TelephoneNum"
         :head-title-name                  "HeadTitle (name)"
         :head-first-name                  "HeadFirstName"
         :head-last-name                   "HeadLastName"
         :head-preferred-job-title         "HeadPreferredJobTitle"
         :bso-inspectorate-name-name       "BSOInspectorateName (name)"
         :inspectorate-report              "InspectorateReport"
         :date-of-last-inspection-visit    "DateOfLastInspectionVisit"
         :next-inspection-visit            "NextInspectionVisit"
         :teen-moth-name                   "TeenMoth (name)"
         :teen-moth-places                 "TeenMothPlaces"
         :ccf-name                         "CCF (name)"
         :senpru-name                      "SENPRU (name)"
         :ebd-name                         "EBD (name)"
         :places-pru                       "PlacesPRU"
         :ft-prov-name                     "FTProv (name)"
         :ed-by-other-name                 "EdByOther (name)"
         :section41-approved-name          "Section41Approved (name)"
         :sen1-name                        "SEN1 (name)"
         :sen2-name                        "SEN2 (name)"
         :sen3-name                        "SEN3 (name)"
         :sen4-name                        "SEN4 (name)"
         :sen5-name                        "SEN5 (name)"
         :sen6-name                        "SEN6 (name)"
         :sen7-name                        "SEN7 (name)"
         :sen8-name                        "SEN8 (name)"
         :sen9-name                        "SEN9 (name)"
         :sen10-name                       "SEN10 (name)"
         :sen11-name                       "SEN11 (name)"
         :sen12-name                       "SEN12 (name)"
         :sen13-name                       "SEN13 (name)"
         :type-of-resourced-provision-name "TypeOfResourcedProvision (name)"
         :resourced-provision-on-roll      "ResourcedProvisionOnRoll"
         :resourced-provision-capacity     "ResourcedProvisionCapacity"
         :sen-unit-on-roll                 "SenUnitOnRoll"
         :sen-unit-capacity                "SenUnitCapacity"
         :gor-code                         "GOR (code)"
         :gor-name                         "GOR (name)"
         :district-administrative-code     "DistrictAdministrative (code)"
         :district-administrative-name     "DistrictAdministrative (name)"
         :administrative-ward-code         "AdministrativeWard (code)"
         :administrative-ward-name         "AdministrativeWard (name)"
         :parliamentary-constituency-code  "ParliamentaryConstituency (code)"
         :parliamentary-constituency-name  "ParliamentaryConstituency (name)"
         :urban-rural-code                 "UrbanRural (code)"
         :urban-rural-name                 "UrbanRural (name)"
         :gssla-code-name                  "GSSLACode (name)"
         :easting                          "Easting"
         :northing                         "Northing"
         :msoa-name                        "MSOA (name)"
         :lsoa-name                        "LSOA (name)"
         :inspectorate-name-name           "InspectorateName (name)"
         :sen-stat                         "SENStat"
         :sen-no-stat                      "SENNoStat"
         :boarding-establishment-name      "BoardingEstablishment (name)"
         :props-name                       "PropsName"
         :previous-la-code                 "PreviousLA (code)"
         :previous-la-name                 "PreviousLA (name)"
         :previous-establishment-number    "PreviousEstablishmentNumber"
         :ofsted-rating-name               "OfstedRating (name)"
         :rsc-region-name                  "RSCRegion (name)"
         :country-name                     "Country (name)"
         :uprn                             "UPRN"
         :site-name                        "SiteName"
         :qab-name-code                    "QABName (code)"
         :qab-name-name                    "QABName (name)"
         :establishment-accredited-code    "EstablishmentAccredited (code)"
         :establishment-accredited-name    "EstablishmentAccredited (name)"
         :qab-report                       "QABReport"
         :ch-number                        "CHNumber"
         :msoa-code                        "MSOA (code)"
         :lsoa-code                        "LSOA (code)"
         :fsm                              "FSM"}))

(comment
  ;; Keyword column names created from CSV column lables using:
  (map (fn [s] (-> s
                   (clojure.string/replace #"(?<!^)([A-Z])(?=[a-z])" "-$1")
                   (clojure.string/replace #"(?<=[a-z])([A-Z])" "-$1")
                   (clojure.string/replace #"[()]" "")
                   (clojure.string/replace #" +" "-")
                   (clojure.string/lower-case)))
       (vals col-name->csv-label))
  )

(def col-name->label
  "Map column name to label.

  Labels adapted from [www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate](https://www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate).
  "
  (into (sorted-map-by (fn [k1 k2] (compare [(get col-name->order k1) k1]
                                            [(get col-name->order k2) k2]))) 
        {:urn                              "URN"
         :la-code                          "LA (code)"
         :la-name                          "LA"
         :establishment-number             "Establishment Number"
         :establishment-name               "School / College Name"
         :type-of-establishment-code       "Establishment type (code)"
         :type-of-establishment-name       "Establishment type"
         :establishment-type-group-code    "Establishment type group (code)"
         :establishment-type-group-name    "Establishment type group"
         :establishment-status-code        "Establishment status (code)"
         :establishment-status-name        "Establishment status"
         :reason-establishment-opened-code "Reason establishment opened (code)"
         :reason-establishment-opened-name "Reason establishment opened"
         :open-date                        "Open date"
         :reason-establishment-closed-code "Reason establishment closed (code)"
         :reason-establishment-closed-name "Reason establishment closed"
         :close-date                       "Close date"
         :phase-of-education-code          "Phase of education (code)"
         :phase-of-education-name          "Phase of education"
         :statutory-low-age                "Age range (low)"
         :statutory-high-age               "Age range (high)"
         :boarders-code                    "Boarders (code)"
         :boarders-name                    "Boarders"
         :nursery-provision-name           "Nursery provision"
         :official-sixth-form-code         "Official sixth form (code)"
         :official-sixth-form-name         "Official sixth form"
         :gender-code                      "Gender of entry (code)"
         :gender-name                      "Gender of entry"
         :religious-character-code         "Religious character (code)"
         :religious-character-name         "Religious character"
         :religious-ethos-name             "Religious ethos"
         :diocese-code                     "Diocese (code)"
         :diocese-name                     "Diocese"
         :admissions-policy-code           "Admissons policy (code)"
         :admissions-policy-name           "Admissons policy"
         :school-capacity                  "School capacity"
         :special-classes-code             "Special classes (code)"
         :special-classes-name             "Special classes"
         :census-date                      "Census date"
         :number-of-pupils                 "Number of pupils"
         :number-of-boys                   "Number of boys"
         :number-of-girls                  "Number of girls"
         :percentage-fsm                   "Percentage FSM"
         :trust-school-flag-code           "Trust school flag (code)"
         :trust-school-flag-name           "Trust school flag"
         :trusts-code                      "Academy trust or trust (code)"
         :trusts-name                      "Academy trust or trust"
         :school-sponsor-flag-name         "School sponsor flag"
         :school-sponsors-name             "Academy sponsor"
         :federation-flag-name             "Federation flag"
         :federations-code                 "Federation (code)"
         :federations-name                 "Federation"
         :ukprn                            "UK provider reference number (UKPRN)"
         :fehe-identifier                  "FEHE identifier"
         :further-education-type-name      "Further education type"
         :ofsted-last-insp                 "Date of last OFSTED inspection"
         :ofsted-special-measures-code     "OFSTED special measures (code)"
         :ofsted-special-measures-name     "OFSTED special measures"
         :last-changed-date                "Last Changed Date"
         :street                           "Street"
         :locality                         "Locality"
         :address3                         "Address 3"
         :town                             "Town"
         :county-name                      "County"
         :postcode                         "Postcode"
         :school-website                   "Website"
         :telephone-num                    "Telephone"
         :head-title-name                  "Headteacher/Principal title"
         :head-first-name                  "Headteacher/Principal first name"
         :head-last-name                   "Headteacher/Principal last name"
         :head-preferred-job-title         "Headteacher/Principal preferred job title"
         :bso-inspectorate-name-name       "BSO inspectorate name"
         :inspectorate-report              "Inspectorate report URL"
         :date-of-last-inspection-visit    "Date of last inspection visit"
         :next-inspection-visit            "Date of next inspection visit"
         :teen-moth-name                   "Teenage mothers"
         :teen-moth-places                 "Teenage mothers capacity"
         :ccf-name                         "Child Care Facilities"
         :senpru-name                      "PRU provision for SEN"
         :ebd-name                         "PRU provision for EBD"
         :places-pru                       "Number of PRU places"
         :ft-prov-name                     "PRU offer full time provision"
         :ed-by-other-name                 "PRU offer tuition by anther provider"
         :section41-approved-name          "Section41Approved"
         :sen1-name                        "SEN need 1"
         :sen2-name                        "SEN need 2"
         :sen3-name                        "SEN need 3"
         :sen4-name                        "SEN need 4"
         :sen5-name                        "SEN need 5"
         :sen6-name                        "SEN need 6"
         :sen7-name                        "SEN need 7"
         :sen8-name                        "SEN need 8"
         :sen9-name                        "SEN need 9"
         :sen10-name                       "SEN need 10"
         :sen11-name                       "SEN need 11"
         :sen12-name                       "SEN need 12"
         :sen13-name                       "SEN need 13"
         :type-of-resourced-provision-name "Type of resourced provision"
         :resourced-provision-on-roll      "Resourced provision number on roll"
         :resourced-provision-capacity     "Resourced provision capacity"
         :sen-unit-on-roll                 "SEN unit number on roll"
         :sen-unit-capacity                "SEN unit capacity"
         :gor-code                         "GOR (code)"
         :gor-name                         "GOR"
         :district-administrative-code     "District administrative (code)"
         :district-administrative-name     "District administrative"
         :administrative-ward-code         "Administrative ward (code)"
         :administrative-ward-name         "Administrative ward"
         :parliamentary-constituency-code  "Parliamentary constituency (code)"
         :parliamentary-constituency-name  "Parliamentary constituency"
         :urban-rural-code                 "Urban rural (code)"
         :urban-rural-name                 "Urban rural"
         :gssla-code-name                  "GSSLA code"
         :easting                          "Easting"
         :northing                         "Northing"
         :msoa-name                        "MSOA"
         :lsoa-name                        "LSOA"
         :inspectorate-name-name           "Inspectorate name"
         :sen-stat                         "Number of special pupils under a SEN statement or EHCP"
         :sen-no-stat                      "Number of special pupils not under a SEN statement or EHCP"
         :boarding-establishment-name      "Boarding establishment"
         :props-name                       "Proprietor's name"
         :previous-la-code                 "Previous local authority (code)"
         :previous-la-name                 "Previous local authority"
         :previous-establishment-number    "Previous establishment number"
         :ofsted-rating-name               "OFSTED rating"
         :rsc-region-name                  "RSC region"
         :country-name                     "Country"
         :uprn                             "UPRN"
         :site-name                        "Site name"
         :qab-name-code                    "QAB name (code)"
         :qab-name-name                    "QAB name"
         :establishment-accredited-code    "Establishment accredited (code)"
         :establishment-accredited-name    "Establishment accredited"
         :qab-report                       "QAB report"
         :ch-number                        "CH number"
         :msoa-code                        "MSOA (code)"
         :lsoa-code                        "LSOA (code)"
         :fsm                              "FSM"}))



;;; # Read establishment data
(def establishments
  "GIAS all establishment data as a (delayed) dataset with keyword column names."
  (let [parse-date (fn [s] (java.time.LocalDate/parse s (java.time.format.DateTimeFormatter/ofPattern "dd-MM-uuuu")))]
    (delay (with-open [in (-> data-file-name
                              io/resource
                              io/file
                              io/input-stream)]
             (ds/->dataset in {:file-type    :csv
                               :separator    ","
                               :dataset-name (re-find #".+(?=\.csv$)" data-file-name)
                               :header-row?  true
                               :key-fn       #((set/map-invert col-name->csv-label) % %)
                               :parser-fn    {:urn                           :string
                                              :la-code                       :string
                                              :establishment-number          :string
                                              :open-date                     [:packed-local-date parse-date]
                                              :close-date                    [:packed-local-date parse-date]
                                              :census-date                   [:packed-local-date parse-date]
                                              :ukprn                         :string
                                              :fehe-identifier               :string
                                              :ofsted-last-insp              [:packed-local-date parse-date]
                                              :last-changed-date             [:packed-local-date parse-date]
                                              :telephone-num                 :string
                                              :date-of-last-inspection-visit [:packed-local-date parse-date]
                                              :next-inspection-visit         [:packed-local-date parse-date]
                                              :previous-la-code              :string
                                              :previous-establishment-number :string
                                              :uprn                          :string
                                              :qab-report                    :string
                                              :ch-number                     :string}})))))


(comment
  (defn- column-info
    "Selected column info, in column order"
    [ds]
    (let [column-name->order (zipmap (tc/column-names ds) (iterate inc 1))]
      (-> ds
          (tc/info)
          (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
          (tc/order-by #(column-name->order (:col-name %))))))

  (column-info @establishments)
  ;; => edubasealldata20230421: descriptive-stats [139 6]:
  ;;    |                         :col-name |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |            :string |    50078 |          0 |            |            |
  ;;    |                          :la-code |            :string |    50078 |          0 |            |            |
  ;;    |                          :la-name |            :string |    50078 |          0 |            |            |
  ;;    |             :establishment-number |            :string |    49943 |        135 |            |            |
  ;;    |               :establishment-name |            :string |    50078 |          0 |            |            |
  ;;    |       :type-of-establishment-code |             :int16 |    50078 |          0 |      1.000 |      56.00 |
  ;;    |       :type-of-establishment-name |            :string |    50078 |          0 |            |            |
  ;;    |    :establishment-type-group-code |             :int16 |    50078 |          0 |      1.000 |      13.00 |
  ;;    |    :establishment-type-group-name |            :string |    50078 |          0 |            |            |
  ;;    |        :establishment-status-code |             :int16 |    50078 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |            :string |    50078 |          0 |            |            |
  ;;    | :reason-establishment-opened-code |             :int16 |    50078 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-opened-name |            :string |    49427 |        651 |            |            |
  ;;    |                        :open-date | :packed-local-date |    19503 |      30575 | 1800-01-01 | 2023-10-01 |
  ;;    | :reason-establishment-closed-code |             :int16 |    50078 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-closed-name |            :string |    38137 |      11941 |            |            |
  ;;    |                       :close-date | :packed-local-date |    22679 |      27399 | 1900-01-01 | 2023-09-30 |
  ;;    |          :phase-of-education-code |             :int16 |    50078 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |            :string |    50078 |          0 |            |            |
  ;;    |                :statutory-low-age |             :int16 |    46115 |       3963 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |             :int16 |    46118 |       3960 |      3.000 |      99.00 |
  ;;    |                    :boarders-code |             :int16 |    50078 |          0 |      0.000 |      9.000 |
  ;;    |                    :boarders-name |            :string |    48514 |       1564 |            |            |
  ;;    |           :nursery-provision-name |            :string |    50052 |         26 |            |            |
  ;;    |         :official-sixth-form-code |             :int16 |    50078 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |            :string |    50071 |          7 |            |            |
  ;;    |                      :gender-code |             :int16 |    50078 |          0 |      0.000 |      9.000 |
  ;;    |                      :gender-name |            :string |    48669 |       1409 |            |            |
  ;;    |         :religious-character-code |             :int16 |    50078 |          0 |      0.000 |      99.00 |
  ;;    |         :religious-character-name |            :string |    46459 |       3619 |            |            |
  ;;    |             :religious-ethos-name |            :string |    46817 |       3261 |            |            |
  ;;    |                     :diocese-code |            :string |    50078 |          0 |            |            |
  ;;    |                     :diocese-name |            :string |    48792 |       1286 |            |            |
  ;;    |           :admissions-policy-code |             :int16 |    50078 |          0 |      0.000 |      9.000 |
  ;;    |           :admissions-policy-name |            :string |    44963 |       5115 |            |            |
  ;;    |                  :school-capacity |             :int16 |    37792 |      12286 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |             :int16 |    50078 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |            :string |    49962 |        116 |            |            |
  ;;    |                      :census-date | :packed-local-date |    28560 |      21518 | 2017-01-19 | 2022-01-20 |
  ;;    |                 :number-of-pupils |             :int16 |    28557 |      21521 |      0.000 |       3442 |
  ;;    |                   :number-of-boys |             :int16 |    28547 |      21531 |      0.000 |       1773 |
  ;;    |                  :number-of-girls |             :int16 |    28542 |      21536 |      0.000 |       1871 |
  ;;    |                   :percentage-fsm |           :float64 |    28352 |      21726 |      0.000 |      100.0 |
  ;;    |           :trust-school-flag-code |             :int16 |    50078 |          0 |      0.000 |      5.000 |
  ;;    |           :trust-school-flag-name |            :string |    50078 |          0 |            |            |
  ;;    |                      :trusts-code |             :int16 |    10842 |      39236 |       1028 |  1.766E+04 |
  ;;    |                      :trusts-name |            :string |    10842 |      39236 |            |            |
  ;;    |         :school-sponsor-flag-name |            :string |    50078 |          0 |            |            |
  ;;    |             :school-sponsors-name |            :string |     7741 |      42337 |            |            |
  ;;    |             :federation-flag-name |            :string |    50078 |          0 |            |            |
  ;;    |                 :federations-code |            :string |     1114 |      48964 |            |            |
  ;;    |                 :federations-name |            :string |     1114 |      48964 |            |            |
  ;;    |                            :ukprn |            :string |    31650 |      18428 |            |            |
  ;;    |                  :fehe-identifier |            :string |      537 |      49541 |            |            |
  ;;    |      :further-education-type-name |            :string |    46317 |       3761 |            |            |
  ;;    |                 :ofsted-last-insp | :packed-local-date |    26420 |      23658 | 2006-05-12 | 2023-01-18 |
  ;;    |     :ofsted-special-measures-code |             :int16 |    50078 |          0 |      0.000 |      0.000 |
  ;;    |     :ofsted-special-measures-name |            :string |    50078 |          0 |            |            |
  ;;    |                :last-changed-date | :packed-local-date |    50078 |          0 | 2013-10-24 | 2023-04-21 |
  ;;    |                           :street |            :string |    48889 |       1189 |            |            |
  ;;    |                         :locality |            :string |    27174 |      22904 |            |            |
  ;;    |                         :address3 |            :string |     3972 |      46106 |            |            |
  ;;    |                             :town |            :string |    48074 |       2004 |            |            |
  ;;    |                      :county-name |            :string |    38552 |      11526 |            |            |
  ;;    |                         :postcode |            :string |    48690 |       1388 |            |            |
  ;;    |                   :school-website |            :string |    24485 |      25593 |            |            |
  ;;    |                    :telephone-num |            :string |    27218 |      22860 |            |            |
  ;;    |                  :head-title-name |            :string |    41092 |       8986 |            |            |
  ;;    |                  :head-first-name |            :string |    42425 |       7653 |            |            |
  ;;    |                   :head-last-name |            :string |    42483 |       7595 |            |            |
  ;;    |         :head-preferred-job-title |            :string |    44462 |       5616 |            |            |
  ;;    |       :bso-inspectorate-name-name |            :string |    50078 |          0 |            |            |
  ;;    |              :inspectorate-report |            :string |      222 |      49856 |            |            |
  ;;    |    :date-of-last-inspection-visit | :packed-local-date |      223 |      49855 | 2016-03-03 | 2023-02-16 |
  ;;    |            :next-inspection-visit | :packed-local-date |        0 |      50078 | 1970-01-01 | 1970-01-01 |
  ;;    |                   :teen-moth-name |            :string |    50070 |          8 |            |            |
  ;;    |                 :teen-moth-places |             :int16 |       99 |      49979 |      0.000 |      45.00 |
  ;;    |                         :ccf-name |            :string |    44555 |       5523 |            |            |
  ;;    |                      :senpru-name |            :string |    50067 |         11 |            |            |
  ;;    |                         :ebd-name |            :string |    50071 |          7 |            |            |
  ;;    |                       :places-pru |             :int16 |      602 |      49476 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |            :string |     1654 |      48424 |            |            |
  ;;    |                 :ed-by-other-name |            :string |    44544 |       5534 |            |            |
  ;;    |          :section41-approved-name |            :string |    50078 |          0 |            |            |
  ;;    |                        :sen1-name |            :string |     5569 |      44509 |            |            |
  ;;    |                        :sen2-name |            :string |     1950 |      48128 |            |            |
  ;;    |                        :sen3-name |            :string |     1164 |      48914 |            |            |
  ;;    |                        :sen4-name |            :string |      810 |      49268 |            |            |
  ;;    |                        :sen5-name |            :string |      586 |      49492 |            |            |
  ;;    |                        :sen6-name |            :string |      508 |      49570 |            |            |
  ;;    |                        :sen7-name |            :string |      451 |      49627 |            |            |
  ;;    |                        :sen8-name |            :string |      378 |      49700 |            |            |
  ;;    |                        :sen9-name |            :string |      297 |      49781 |            |            |
  ;;    |                       :sen10-name |            :string |      203 |      49875 |            |            |
  ;;    |                       :sen11-name |            :string |      140 |      49938 |            |            |
  ;;    |                       :sen12-name |            :string |       95 |      49983 |            |            |
  ;;    |                       :sen13-name |            :string |        7 |      50071 |            |            |
  ;;    | :type-of-resourced-provision-name |            :string |     6904 |      43174 |            |            |
  ;;    |      :resourced-provision-on-roll |             :int16 |     1870 |      48208 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |             :int16 |     1903 |      48175 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |             :int16 |      853 |      49225 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |             :int16 |      872 |      49206 |      0.000 |      427.0 |
  ;;    |                         :gor-code |            :string |    50078 |          0 |            |            |
  ;;    |                         :gor-name |            :string |    50078 |          0 |            |            |
  ;;    |     :district-administrative-code |            :string |    50078 |          0 |            |            |
  ;;    |     :district-administrative-name |            :string |    47649 |       2429 |            |            |
  ;;    |         :administrative-ward-code |            :string |    50078 |          0 |            |            |
  ;;    |         :administrative-ward-name |            :string |    48691 |       1387 |            |            |
  ;;    |  :parliamentary-constituency-code |            :string |    50078 |          0 |            |            |
  ;;    |  :parliamentary-constituency-name |            :string |    48691 |       1387 |            |            |
  ;;    |                 :urban-rural-code |            :string |    50078 |          0 |            |            |
  ;;    |                 :urban-rural-name |            :string |    48690 |       1388 |            |            |
  ;;    |                  :gssla-code-name |            :string |    50078 |          0 |            |            |
  ;;    |                          :easting |             :int32 |    48474 |       1604 |      0.000 |  6.551E+05 |
  ;;    |                         :northing |             :int32 |    48474 |       1604 |      0.000 |  8.119E+05 |
  ;;    |                        :msoa-name |            :string |    48691 |       1387 |            |            |
  ;;    |                        :lsoa-name |            :string |    48690 |       1388 |            |            |
  ;;    |           :inspectorate-name-name |            :string |     4492 |      45586 |            |            |
  ;;    |                         :sen-stat |             :int16 |     3636 |      46442 |      0.000 |      311.0 |
  ;;    |                      :sen-no-stat |             :int16 |     3501 |      46577 |      0.000 |      485.0 |
  ;;    |      :boarding-establishment-name |            :string |     2607 |      47471 |            |            |
  ;;    |                       :props-name |            :string |     3105 |      46973 |            |            |
  ;;    |                 :previous-la-code |            :string |    50078 |          0 |            |            |
  ;;    |                 :previous-la-name |            :string |    16522 |      33556 |            |            |
  ;;    |    :previous-establishment-number |            :string |     8014 |      42064 |            |            |
  ;;    |               :ofsted-rating-name |            :string |    26367 |      23711 |            |            |
  ;;    |                  :rsc-region-name |            :string |    47147 |       2931 |            |            |
  ;;    |                     :country-name |            :string |     2523 |      47555 |            |            |
  ;;    |                             :uprn |            :string |    37411 |      12667 |            |            |
  ;;    |                        :site-name |            :string |        3 |      50075 |            |            |
  ;;    |                    :qab-name-code |             :int16 |    50078 |          0 |      0.000 |      1.000 |
  ;;    |                    :qab-name-name |            :string |    50078 |          0 |            |            |
  ;;    |    :establishment-accredited-code |             :int16 |    50078 |          0 |      0.000 |      2.000 |
  ;;    |    :establishment-accredited-name |            :string |    50078 |          0 |            |            |
  ;;    |                       :qab-report |            :string |        0 |      50078 |            |            |
  ;;    |                        :ch-number |            :string |        0 |      50078 |            |            |
  ;;    |                        :msoa-code |            :string |    50078 |          0 |            |            |
  ;;    |                        :lsoa-code |            :string |    50078 |          0 |            |            |
  ;;    |                              :fsm |             :int16 |    28353 |      21725 |      0.000 |      849.0 |

  )
