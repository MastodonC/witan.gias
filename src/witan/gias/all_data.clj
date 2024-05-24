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
  "edubasealldata20240524.csv")



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
                   :fsm
                   :accreditation-expiry-date]
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
         :fsm                              "FSM"
         :accreditation-expiry-date        "AccreditationExpiryDate"}))

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
         :ccf-name                         "Child care facilities"
         :senpru-name                      "PRU provision for SEN"
         :ebd-name                         "PRU provision for EBD"
         :places-pru                       "Number of PRU places"
         :ft-prov-name                     "PRU offer full time provision"
         :ed-by-other-name                 "PRU offer tuition by anther provider"
         :section41-approved-name          "Section 41 approved"
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
         :fsm                              "FSM"
         :accreditation-expiry-date        "Accreditation expiry date"}))

(def key-col-names-for-send
  "Names of key columns for SEND"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get col-name->order k1) k1]
                              [(get col-name->order k2) k2]))
         [:urn
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
          :open-date
          :close-date
          :phase-of-education-code
          :phase-of-education-name
          :statutory-low-age
          :statutory-high-age
          :nursery-provision-name
          :official-sixth-form-code
          :official-sixth-form-name
          :school-capacity
          :special-classes-code
          :special-classes-name
          :number-of-pupils
          :ukprn
          :further-education-type-name
          :school-website
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
          :sen-stat
          :sen-no-stat
          :uprn
          ]))

;;; # Read establishment data
(defn ->ds
  "Read GIAS all establishment data into a dataset with keyword column names.

  Optional `options` merged into `->dataset` options.
  "
  ([] (->ds {}))
  ([options] (let [parse-date (fn [s] (java.time.LocalDate/parse s (java.time.format.DateTimeFormatter/ofPattern "dd-MM-uuuu")))]
               (with-open [in (-> data-file-name
                                  io/resource
                                  io/file
                                  io/input-stream)]
                 (ds/->dataset in (merge {:file-type    :csv
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
                                                         :ch-number                     :string}}
                                         options))))))

(defn key-cols-for-send->ds
  "Read key columns for SEND from GIAS all establishment data into a dataset with keyword column names.

  Optional `options` merged into `->dataset` options.
  "
  ([] (key-cols-for-send->ds {}))
  ([options] (->ds (merge {:column-whitelist (map col-name->csv-label key-col-names-for-send)
                           :dataset-name     (str (re-find #".+(?=\.csv$)" data-file-name) " - key columns for SEND")}
                          options))))


(comment
  (defn- column-info
    "Selected column info, in column order"
    [ds]
    (let [column-name->order (zipmap (tc/column-names ds) (iterate inc 1))]
      (-> ds
          (tc/info)
          (tc/map-columns :csv-col-name [:col-name] col-name->csv-label)
          (tc/map-columns :col-label [:col-name] col-name->label)
          (tc/select-columns [:col-name :csv-col-name :col-label :datatype :n-valid :n-missing :min :max])
          (tc/order-by #(column-name->order (:col-name %))))))

  (-> (->ds)
      column-info
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20230817: descriptive-stats [140 8]:
  ;;    |                         :col-name |                    :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|----------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |                              URN |                                                        URN |            :string |    50294 |          0 |            |            |
  ;;    |                          :la-code |                        LA (code) |                                                  LA (code) |            :string |    50294 |          0 |            |            |
  ;;    |                          :la-name |                        LA (name) |                                                         LA |            :string |    50294 |          0 |            |            |
  ;;    |             :establishment-number |              EstablishmentNumber |                                       Establishment Number |            :string |    50159 |        135 |            |            |
  ;;    |               :establishment-name |                EstablishmentName |                                      School / College Name |            :string |    50294 |          0 |            |            |
  ;;    |       :type-of-establishment-code |       TypeOfEstablishment (code) |                                  Establishment type (code) |             :int16 |    50294 |          0 |      1.000 |      56.00 |
  ;;    |       :type-of-establishment-name |       TypeOfEstablishment (name) |                                         Establishment type |            :string |    50294 |          0 |            |            |
  ;;    |    :establishment-type-group-code |    EstablishmentTypeGroup (code) |                            Establishment type group (code) |             :int16 |    50294 |          0 |      1.000 |      11.00 |
  ;;    |    :establishment-type-group-name |    EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    50294 |          0 |            |            |
  ;;    |        :establishment-status-code |       EstablishmentStatus (code) |                                Establishment status (code) |             :int16 |    50294 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |       EstablishmentStatus (name) |                                       Establishment status |            :string |    50294 |          0 |            |            |
  ;;    | :reason-establishment-opened-code | ReasonEstablishmentOpened (code) |                         Reason establishment opened (code) |             :int16 |    50294 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-opened-name | ReasonEstablishmentOpened (name) |                                Reason establishment opened |            :string |    49612 |        682 |            |            |
  ;;    |                        :open-date |                         OpenDate |                                                  Open date | :packed-local-date |    19781 |      30513 | 1800-01-01 | 2024-01-01 |
  ;;    | :reason-establishment-closed-code | ReasonEstablishmentClosed (code) |                         Reason establishment closed (code) |             :int16 |    50294 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-closed-name | ReasonEstablishmentClosed (name) |                                Reason establishment closed |            :string |    38028 |      12266 |            |            |
  ;;    |                       :close-date |                        CloseDate |                                                 Close date | :packed-local-date |    22874 |      27420 | 1900-01-01 | 2026-08-31 |
  ;;    |          :phase-of-education-code |          PhaseOfEducation (code) |                                  Phase of education (code) |             :int16 |    50294 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |          PhaseOfEducation (name) |                                         Phase of education |            :string |    50294 |          0 |            |            |
  ;;    |                :statutory-low-age |                  StatutoryLowAge |                                            Age range (low) |             :int16 |    46332 |       3962 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |                 StatutoryHighAge |                                           Age range (high) |             :int16 |    46335 |       3959 |      3.000 |      99.00 |
  ;;    |                    :boarders-code |                  Boarders (code) |                                            Boarders (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |                    :boarders-name |                  Boarders (name) |                                                   Boarders |            :string |    48683 |       1611 |            |            |
  ;;    |           :nursery-provision-name |          NurseryProvision (name) |                                          Nursery provision |            :string |    50268 |         26 |            |            |
  ;;    |         :official-sixth-form-code |         OfficialSixthForm (code) |                                 Official sixth form (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |         OfficialSixthForm (name) |                                        Official sixth form |            :string |    50287 |          7 |            |            |
  ;;    |                      :gender-code |                    Gender (code) |                                     Gender of entry (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |                      :gender-name |                    Gender (name) |                                            Gender of entry |            :string |    48885 |       1409 |            |            |
  ;;    |         :religious-character-code |        ReligiousCharacter (code) |                                 Religious character (code) |             :int16 |    50294 |          0 |      0.000 |      99.00 |
  ;;    |         :religious-character-name |        ReligiousCharacter (name) |                                        Religious character |            :string |    46620 |       3674 |            |            |
  ;;    |             :religious-ethos-name |            ReligiousEthos (name) |                                            Religious ethos |            :string |    47029 |       3265 |            |            |
  ;;    |                     :diocese-code |                   Diocese (code) |                                             Diocese (code) |            :string |    50294 |          0 |            |            |
  ;;    |                     :diocese-name |                   Diocese (name) |                                                    Diocese |            :string |    48990 |       1304 |            |            |
  ;;    |           :admissions-policy-code |          AdmissionsPolicy (code) |                                    Admissons policy (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |           :admissions-policy-name |          AdmissionsPolicy (name) |                                           Admissons policy |            :string |    45105 |       5189 |            |            |
  ;;    |                  :school-capacity |                   SchoolCapacity |                                            School capacity |             :int16 |    37979 |      12315 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |            SpecialClasses (code) |                                     Special classes (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |            SpecialClasses (name) |                                            Special classes |            :string |    50173 |        121 |            |            |
  ;;    |                      :census-date |                       CensusDate |                                                Census date | :packed-local-date |    28560 |      21734 | 2017-01-19 | 2022-01-20 |
  ;;    |                 :number-of-pupils |                   NumberOfPupils |                                           Number of pupils |             :int16 |    28560 |      21734 |      0.000 |       3442 |
  ;;    |                   :number-of-boys |                     NumberOfBoys |                                             Number of boys |             :int16 |    28547 |      21747 |      0.000 |       1773 |
  ;;    |                  :number-of-girls |                    NumberOfGirls |                                            Number of girls |             :int16 |    28542 |      21752 |      0.000 |       1871 |
  ;;    |                   :percentage-fsm |                    PercentageFSM |                                             Percentage FSM |           :float64 |    28352 |      21942 |      0.000 |      100.0 |
  ;;    |           :trust-school-flag-code |           TrustSchoolFlag (code) |                                   Trust school flag (code) |             :int16 |    50294 |          0 |      0.000 |      5.000 |
  ;;    |           :trust-school-flag-name |           TrustSchoolFlag (name) |                                          Trust school flag |            :string |    50294 |          0 |            |            |
  ;;    |                      :trusts-code |                    Trusts (code) |                              Academy trust or trust (code) |             :int16 |    10900 |      39394 |       1028 |  1.766E+04 |
  ;;    |                      :trusts-name |                    Trusts (name) |                                     Academy trust or trust |            :string |    10900 |      39394 |            |            |
  ;;    |         :school-sponsor-flag-name |         SchoolSponsorFlag (name) |                                        School sponsor flag |            :string |    50294 |          0 |            |            |
  ;;    |             :school-sponsors-name |            SchoolSponsors (name) |                                            Academy sponsor |            :string |     7861 |      42433 |            |            |
  ;;    |             :federation-flag-name |            FederationFlag (name) |                                            Federation flag |            :string |    50294 |          0 |            |            |
  ;;    |                 :federations-code |               Federations (code) |                                          Federation (code) |            :string |     1128 |      49166 |            |            |
  ;;    |                 :federations-name |               Federations (name) |                                                 Federation |            :string |     1128 |      49166 |            |            |
  ;;    |                            :ukprn |                            UKPRN |                       UK provider reference number (UKPRN) |            :string |    31800 |      18494 |            |            |
  ;;    |                  :fehe-identifier |                   FEHEIdentifier |                                            FEHE identifier |            :string |      537 |      49757 |            |            |
  ;;    |      :further-education-type-name |      FurtherEducationType (name) |                                     Further education type |            :string |    46315 |       3979 |            |            |
  ;;    |                 :ofsted-last-insp |                   OfstedLastInsp |                             Date of last OFSTED inspection | :packed-local-date |    26967 |      23327 | 2006-05-12 | 2023-05-25 |
  ;;    |     :ofsted-special-measures-code |     OfstedSpecialMeasures (code) |                             OFSTED special measures (code) |             :int16 |    50294 |          0 |      0.000 |      0.000 |
  ;;    |     :ofsted-special-measures-name |     OfstedSpecialMeasures (name) |                                    OFSTED special measures |            :string |    50294 |          0 |            |            |
  ;;    |                :last-changed-date |                  LastChangedDate |                                          Last Changed Date | :packed-local-date |    50294 |          0 | 2013-10-24 | 2023-08-16 |
  ;;    |                           :street |                           Street |                                                     Street |            :string |    49100 |       1194 |            |            |
  ;;    |                         :locality |                         Locality |                                                   Locality |            :string |    27266 |      23028 |            |            |
  ;;    |                         :address3 |                         Address3 |                                                  Address 3 |            :string |     3989 |      46305 |            |            |
  ;;    |                             :town |                             Town |                                                       Town |            :string |    48291 |       2003 |            |            |
  ;;    |                      :county-name |                    County (name) |                                                     County |            :string |    38716 |      11578 |            |            |
  ;;    |                         :postcode |                         Postcode |                                                   Postcode |            :string |    48876 |       1418 |            |            |
  ;;    |                   :school-website |                    SchoolWebsite |                                                    Website |            :string |    24530 |      25764 |            |            |
  ;;    |                    :telephone-num |                     TelephoneNum |                                                  Telephone |            :string |    27246 |      23048 |            |            |
  ;;    |                  :head-title-name |                 HeadTitle (name) |                                Headteacher/Principal title |            :string |    41284 |       9010 |            |            |
  ;;    |                  :head-first-name |                    HeadFirstName |                           Headteacher/Principal first name |            :string |    42643 |       7651 |            |            |
  ;;    |                   :head-last-name |                     HeadLastName |                            Headteacher/Principal last name |            :string |    42701 |       7593 |            |            |
  ;;    |         :head-preferred-job-title |            HeadPreferredJobTitle |                  Headteacher/Principal preferred job title |            :string |    44518 |       5776 |            |            |
  ;;    |       :bso-inspectorate-name-name |       BSOInspectorateName (name) |                                      BSO inspectorate name |            :string |    50294 |          0 |            |            |
  ;;    |              :inspectorate-report |               InspectorateReport |                                    Inspectorate report URL |            :string |      248 |      50046 |            |            |
  ;;    |    :date-of-last-inspection-visit |        DateOfLastInspectionVisit |                              Date of last inspection visit | :packed-local-date |      250 |      50044 | 2016-03-03 | 2023-06-12 |
  ;;    |            :next-inspection-visit |              NextInspectionVisit |                              Date of next inspection visit | :packed-local-date |        0 |      50294 | 1970-01-01 | 1970-01-01 |
  ;;    |                   :teen-moth-name |                  TeenMoth (name) |                                            Teenage mothers |            :string |    50286 |          8 |            |            |
  ;;    |                 :teen-moth-places |                   TeenMothPlaces |                                   Teenage mothers capacity |             :int16 |       99 |      50195 |      0.000 |      45.00 |
  ;;    |                         :ccf-name |                       CCF (name) |                                      Child care facilities |            :string |    44613 |       5681 |            |            |
  ;;    |                      :senpru-name |                    SENPRU (name) |                                      PRU provision for SEN |            :string |    50283 |         11 |            |            |
  ;;    |                         :ebd-name |                       EBD (name) |                                      PRU provision for EBD |            :string |    50287 |          7 |            |            |
  ;;    |                       :places-pru |                        PlacesPRU |                                       Number of PRU places |             :int16 |      602 |      49692 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |                    FTProv (name) |                              PRU offer full time provision |            :string |     1714 |      48580 |            |            |
  ;;    |                 :ed-by-other-name |                 EdByOther (name) |                       PRU offer tuition by anther provider |            :string |    44602 |       5692 |            |            |
  ;;    |          :section41-approved-name |         Section41Approved (name) |                                        Section 41 approved |            :string |    50294 |          0 |            |            |
  ;;    |                        :sen1-name |                      SEN1 (name) |                                                 SEN need 1 |            :string |     5582 |      44712 |            |            |
  ;;    |                        :sen2-name |                      SEN2 (name) |                                                 SEN need 2 |            :string |     1960 |      48334 |            |            |
  ;;    |                        :sen3-name |                      SEN3 (name) |                                                 SEN need 3 |            :string |     1171 |      49123 |            |            |
  ;;    |                        :sen4-name |                      SEN4 (name) |                                                 SEN need 4 |            :string |      813 |      49481 |            |            |
  ;;    |                        :sen5-name |                      SEN5 (name) |                                                 SEN need 5 |            :string |      588 |      49706 |            |            |
  ;;    |                        :sen6-name |                      SEN6 (name) |                                                 SEN need 6 |            :string |      511 |      49783 |            |            |
  ;;    |                        :sen7-name |                      SEN7 (name) |                                                 SEN need 7 |            :string |      455 |      49839 |            |            |
  ;;    |                        :sen8-name |                      SEN8 (name) |                                                 SEN need 8 |            :string |      383 |      49911 |            |            |
  ;;    |                        :sen9-name |                      SEN9 (name) |                                                 SEN need 9 |            :string |      301 |      49993 |            |            |
  ;;    |                       :sen10-name |                     SEN10 (name) |                                                SEN need 10 |            :string |      205 |      50089 |            |            |
  ;;    |                       :sen11-name |                     SEN11 (name) |                                                SEN need 11 |            :string |      142 |      50152 |            |            |
  ;;    |                       :sen12-name |                     SEN12 (name) |                                                SEN need 12 |            :string |       97 |      50197 |            |            |
  ;;    |                       :sen13-name |                     SEN13 (name) |                                                SEN need 13 |            :string |        5 |      50289 |            |            |
  ;;    | :type-of-resourced-provision-name |  TypeOfResourcedProvision (name) |                                Type of resourced provision |            :string |     6986 |      43308 |            |            |
  ;;    |      :resourced-provision-on-roll |         ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     1887 |      48407 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |       ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     1920 |      48374 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |                    SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      866 |      49428 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |                  SenUnitCapacity |                                          SEN unit capacity |             :int16 |      885 |      49409 |      0.000 |      427.0 |
  ;;    |                         :gor-code |                       GOR (code) |                                                 GOR (code) |            :string |    50294 |          0 |            |            |
  ;;    |                         :gor-name |                       GOR (name) |                                                        GOR |            :string |    50294 |          0 |            |            |
  ;;    |     :district-administrative-code |    DistrictAdministrative (code) |                             District administrative (code) |            :string |    50294 |          0 |            |            |
  ;;    |     :district-administrative-name |    DistrictAdministrative (name) |                                    District administrative |            :string |    48872 |       1422 |            |            |
  ;;    |         :administrative-ward-code |        AdministrativeWard (code) |                                 Administrative ward (code) |            :string |    50294 |          0 |            |            |
  ;;    |         :administrative-ward-name |        AdministrativeWard (name) |                                        Administrative ward |            :string |    48873 |       1421 |            |            |
  ;;    |  :parliamentary-constituency-code | ParliamentaryConstituency (code) |                          Parliamentary constituency (code) |            :string |    50294 |          0 |            |            |
  ;;    |  :parliamentary-constituency-name | ParliamentaryConstituency (name) |                                 Parliamentary constituency |            :string |    48873 |       1421 |            |            |
  ;;    |                 :urban-rural-code |                UrbanRural (code) |                                         Urban rural (code) |            :string |    50294 |          0 |            |            |
  ;;    |                 :urban-rural-name |                UrbanRural (name) |                                                Urban rural |            :string |    48872 |       1422 |            |            |
  ;;    |                  :gssla-code-name |                 GSSLACode (name) |                                                 GSSLA code |            :string |    50294 |          0 |            |            |
  ;;    |                          :easting |                          Easting |                                                    Easting |             :int32 |    48592 |       1702 |      0.000 |  6.551E+05 |
  ;;    |                         :northing |                         Northing |                                                   Northing |             :int32 |    48592 |       1702 |      0.000 |  8.119E+05 |
  ;;    |                        :msoa-name |                      MSOA (name) |                                                       MSOA |            :string |    48873 |       1421 |            |            |
  ;;    |                        :lsoa-name |                      LSOA (name) |                                                       LSOA |            :string |    48872 |       1422 |            |            |
  ;;    |           :inspectorate-name-name |          InspectorateName (name) |                                          Inspectorate name |            :string |     4582 |      45712 |            |            |
  ;;    |                         :sen-stat |                          SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3650 |      46644 |      0.000 |      311.0 |
  ;;    |                      :sen-no-stat |                        SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3506 |      46788 |      0.000 |      485.0 |
  ;;    |      :boarding-establishment-name |     BoardingEstablishment (name) |                                     Boarding establishment |            :string |     2608 |      47686 |            |            |
  ;;    |                       :props-name |                        PropsName |                                          Proprietor's name |            :string |     3174 |      47120 |            |            |
  ;;    |                 :previous-la-code |                PreviousLA (code) |                            Previous local authority (code) |            :string |    50294 |          0 |            |            |
  ;;    |                 :previous-la-name |                PreviousLA (name) |                                   Previous local authority |            :string |    16549 |      33745 |            |            |
  ;;    |    :previous-establishment-number |      PreviousEstablishmentNumber |                              Previous establishment number |            :string |     8027 |      42267 |            |            |
  ;;    |               :ofsted-rating-name |              OfstedRating (name) |                                              OFSTED rating |            :string |    26908 |      23386 |            |            |
  ;;    |                  :rsc-region-name |                 RSCRegion (name) |                                                 RSC region |            :string |    47332 |       2962 |            |            |
  ;;    |                     :country-name |                   Country (name) |                                                    Country |            :string |     2702 |      47592 |            |            |
  ;;    |                             :uprn |                             UPRN |                                                       UPRN |            :string |    37600 |      12694 |            |            |
  ;;    |                        :site-name |                         SiteName |                                                  Site name |            :string |        3 |      50291 |            |            |
  ;;    |                    :qab-name-code |                   QABName (code) |                                            QAB name (code) |             :int16 |    50294 |          0 |      0.000 |      0.000 |
  ;;    |                    :qab-name-name |                   QABName (name) |                                                   QAB name |            :string |    50294 |          0 |            |            |
  ;;    |    :establishment-accredited-code |   EstablishmentAccredited (code) |                            Establishment accredited (code) |             :int16 |    50294 |          0 |      0.000 |      0.000 |
  ;;    |    :establishment-accredited-name |   EstablishmentAccredited (name) |                                   Establishment accredited |            :string |    50294 |          0 |            |            |
  ;;    |                       :qab-report |                        QABReport |                                                 QAB report |            :string |        0 |      50294 |            |            |
  ;;    |                        :ch-number |                         CHNumber |                                                  CH number |            :string |        0 |      50294 |            |            |
  ;;    |                        :msoa-code |                      MSOA (code) |                                                MSOA (code) |            :string |    50294 |          0 |            |            |
  ;;    |                        :lsoa-code |                      LSOA (code) |                                                LSOA (code) |            :string |    50294 |          0 |            |            |
  ;;    |                              :fsm |                              FSM |                                                        FSM |             :int16 |    28353 |      21941 |      0.000 |      849.0 |
  ;;    |        :accreditation-expiry-date |          AccreditationExpiryDate |                                  Accreditation expiry date |           :boolean |        0 |      50294 |            |            |

  (-> (->ds {:dataset-name "GIAS establishments"})
      tc/dataset-name)
  ;; => "GIAS establishments"

  (-> (key-cols-for-send->ds)
      column-info
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20230817 - key columns for SEND: descriptive-stats [54 8]:
  ;;    |                         :col-name |                   :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|---------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |                             URN |                                                        URN |            :string |    50294 |          0 |            |            |
  ;;    |                          :la-code |                       LA (code) |                                                  LA (code) |            :string |    50294 |          0 |            |            |
  ;;    |                          :la-name |                       LA (name) |                                                         LA |            :string |    50294 |          0 |            |            |
  ;;    |             :establishment-number |             EstablishmentNumber |                                       Establishment Number |            :string |    50159 |        135 |            |            |
  ;;    |               :establishment-name |               EstablishmentName |                                      School / College Name |            :string |    50294 |          0 |            |            |
  ;;    |       :type-of-establishment-code |      TypeOfEstablishment (code) |                                  Establishment type (code) |             :int16 |    50294 |          0 |      1.000 |      56.00 |
  ;;    |       :type-of-establishment-name |      TypeOfEstablishment (name) |                                         Establishment type |            :string |    50294 |          0 |            |            |
  ;;    |    :establishment-type-group-code |   EstablishmentTypeGroup (code) |                            Establishment type group (code) |             :int16 |    50294 |          0 |      1.000 |      11.00 |
  ;;    |    :establishment-type-group-name |   EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    50294 |          0 |            |            |
  ;;    |        :establishment-status-code |      EstablishmentStatus (code) |                                Establishment status (code) |             :int16 |    50294 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |      EstablishmentStatus (name) |                                       Establishment status |            :string |    50294 |          0 |            |            |
  ;;    |                        :open-date |                        OpenDate |                                                  Open date | :packed-local-date |    19781 |      30513 | 1800-01-01 | 2024-01-01 |
  ;;    |                       :close-date |                       CloseDate |                                                 Close date | :packed-local-date |    22874 |      27420 | 1900-01-01 | 2026-08-31 |
  ;;    |          :phase-of-education-code |         PhaseOfEducation (code) |                                  Phase of education (code) |             :int16 |    50294 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |         PhaseOfEducation (name) |                                         Phase of education |            :string |    50294 |          0 |            |            |
  ;;    |                :statutory-low-age |                 StatutoryLowAge |                                            Age range (low) |             :int16 |    46332 |       3962 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |                StatutoryHighAge |                                           Age range (high) |             :int16 |    46335 |       3959 |      3.000 |      99.00 |
  ;;    |           :nursery-provision-name |         NurseryProvision (name) |                                          Nursery provision |            :string |    50268 |         26 |            |            |
  ;;    |         :official-sixth-form-code |        OfficialSixthForm (code) |                                 Official sixth form (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |        OfficialSixthForm (name) |                                        Official sixth form |            :string |    50287 |          7 |            |            |
  ;;    |                  :school-capacity |                  SchoolCapacity |                                            School capacity |             :int16 |    37979 |      12315 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |           SpecialClasses (code) |                                     Special classes (code) |             :int16 |    50294 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |           SpecialClasses (name) |                                            Special classes |            :string |    50173 |        121 |            |            |
  ;;    |                 :number-of-pupils |                  NumberOfPupils |                                           Number of pupils |             :int16 |    28560 |      21734 |      0.000 |       3442 |
  ;;    |                            :ukprn |                           UKPRN |                       UK provider reference number (UKPRN) |            :string |    31800 |      18494 |            |            |
  ;;    |      :further-education-type-name |     FurtherEducationType (name) |                                     Further education type |            :string |    46315 |       3979 |            |            |
  ;;    |                   :school-website |                   SchoolWebsite |                                                    Website |            :string |    24530 |      25764 |            |            |
  ;;    |                      :senpru-name |                   SENPRU (name) |                                      PRU provision for SEN |            :string |    50283 |         11 |            |            |
  ;;    |                         :ebd-name |                      EBD (name) |                                      PRU provision for EBD |            :string |    50287 |          7 |            |            |
  ;;    |                       :places-pru |                       PlacesPRU |                                       Number of PRU places |             :int16 |      602 |      49692 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |                   FTProv (name) |                              PRU offer full time provision |            :string |     1714 |      48580 |            |            |
  ;;    |                 :ed-by-other-name |                EdByOther (name) |                       PRU offer tuition by anther provider |            :string |    44602 |       5692 |            |            |
  ;;    |          :section41-approved-name |        Section41Approved (name) |                                        Section 41 approved |            :string |    50294 |          0 |            |            |
  ;;    |                        :sen1-name |                     SEN1 (name) |                                                 SEN need 1 |            :string |     5582 |      44712 |            |            |
  ;;    |                        :sen2-name |                     SEN2 (name) |                                                 SEN need 2 |            :string |     1960 |      48334 |            |            |
  ;;    |                        :sen3-name |                     SEN3 (name) |                                                 SEN need 3 |            :string |     1171 |      49123 |            |            |
  ;;    |                        :sen4-name |                     SEN4 (name) |                                                 SEN need 4 |            :string |      813 |      49481 |            |            |
  ;;    |                        :sen5-name |                     SEN5 (name) |                                                 SEN need 5 |            :string |      588 |      49706 |            |            |
  ;;    |                        :sen6-name |                     SEN6 (name) |                                                 SEN need 6 |            :string |      511 |      49783 |            |            |
  ;;    |                        :sen7-name |                     SEN7 (name) |                                                 SEN need 7 |            :string |      455 |      49839 |            |            |
  ;;    |                        :sen8-name |                     SEN8 (name) |                                                 SEN need 8 |            :string |      383 |      49911 |            |            |
  ;;    |                        :sen9-name |                     SEN9 (name) |                                                 SEN need 9 |            :string |      301 |      49993 |            |            |
  ;;    |                       :sen10-name |                    SEN10 (name) |                                                SEN need 10 |            :string |      205 |      50089 |            |            |
  ;;    |                       :sen11-name |                    SEN11 (name) |                                                SEN need 11 |            :string |      142 |      50152 |            |            |
  ;;    |                       :sen12-name |                    SEN12 (name) |                                                SEN need 12 |            :string |       97 |      50197 |            |            |
  ;;    |                       :sen13-name |                    SEN13 (name) |                                                SEN need 13 |            :string |        5 |      50289 |            |            |
  ;;    | :type-of-resourced-provision-name | TypeOfResourcedProvision (name) |                                Type of resourced provision |            :string |     6986 |      43308 |            |            |
  ;;    |      :resourced-provision-on-roll |        ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     1887 |      48407 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |      ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     1920 |      48374 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |                   SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      866 |      49428 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |                 SenUnitCapacity |                                          SEN unit capacity |             :int16 |      885 |      49409 |      0.000 |      427.0 |
  ;;    |                         :sen-stat |                         SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3650 |      46644 |      0.000 |      311.0 |
  ;;    |                      :sen-no-stat |                       SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3506 |      46788 |      0.000 |      485.0 |
  ;;    |                             :uprn |                            UPRN |                                                       UPRN |            :string |    37600 |      12694 |            |            |

  )


(comment
  ;; Write distinct establishment types to data file
  (-> (->ds {:column-whitelist (map col-name->csv-label [:type-of-establishment-code    :type-of-establishment-name
                                                         :establishment-type-group-code :establishment-type-group-name])
             :dataset-name     (str (re-find #".+(?=\.csv$)" data-file-name) "-establishment-types")})
      (tc/unique-by)
      (tc/order-by [:type-of-establishment-code])
      (as-> $
          (tc/write! $ (str "./data/" (tc/dataset-name $) ".csv"))))
  )

