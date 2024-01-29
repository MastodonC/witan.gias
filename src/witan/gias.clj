(ns witan.gias
  "Read in \"Get Information About Schools\" (GIAS) all establishment data from CSV file downloaded from [get-information-schools.service.gov.uk/Downloads](https://www.get-information-schools.service.gov.uk/Downloads)"
  (:require [clojure.java.io :as io]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]))



;;; # Data files
(def default-edubaseall-resource-file-name
  "Name of default edubaseall resource file containing all establishment data"
  "edubasealldata20240129.csv")



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(defn parse-date
  [s]
  (java.time.LocalDate/parse s (java.time.format.DateTimeFormatter/ofPattern "dd-MM-uuuu")))

(defn- parse-sen-provision-type-name
  "Parse contents of \"SEN# (name)\" columns into standard (upper-case) EHCP primary need abbreviations"
  [s]
  (get {"Not Applicable"                                   ::ds/missing ; Note missing
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
        "OTH - Other Difficulty/Disability"                "OTH"} s s))


;;; # edubaseall
;;; ## Column names, labels and types
(def edubaseall-csv-columns
  "Map (ordered) edubaseall CSV column names to (maps of) metadata.
   Labels adapted from [www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate](https://www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate)."
  (as-> [{:csv-col-name "URN"
          :col-name     :urn
          :col-label    "URN"
          :parser-fn    :string}
         {:csv-col-name "LA (code)"
          :col-name     :la-code
          :col-label    "LA (code)"
          :parser-fn    :string}
         {:csv-col-name "LA (name)"
          :col-name     :la-name
          :col-label    "LA"}
         {:csv-col-name "EstablishmentNumber"
          :col-name     :establishment-number
          :col-label    "Establishment Number"
          :parser-fn    :string}
         {:csv-col-name "EstablishmentName"
          :col-name     :establishment-name
          :col-label    "School / College Name"}
         {:csv-col-name "TypeOfEstablishment (code)"
          :col-name     :type-of-establishment-code
          :col-label    "Establishment type (code)"}
         {:csv-col-name "TypeOfEstablishment (name)"
          :col-name     :type-of-establishment-name
          :col-label    "Establishment type"}
         {:csv-col-name "EstablishmentTypeGroup (code)"
          :col-name     :establishment-type-group-code
          :col-label    "Establishment type group (code)"}
         {:csv-col-name "EstablishmentTypeGroup (name)"
          :col-name     :establishment-type-group-name
          :col-label    "Establishment type group"}
         {:csv-col-name "EstablishmentStatus (code)"
          :col-name     :establishment-status-code
          :col-label    "Establishment status (code)"}
         {:csv-col-name "EstablishmentStatus (name)"
          :col-name     :establishment-status-name
          :col-label    "Establishment status"}
         {:csv-col-name "ReasonEstablishmentOpened (code)"
          :col-name     :reason-establishment-opened-code
          :col-label    "Reason establishment opened (code)"}
         {:csv-col-name "ReasonEstablishmentOpened (name)"
          :col-name     :reason-establishment-opened-name
          :col-label    "Reason establishment opened"}
         {:csv-col-name "OpenDate"
          :col-name     :open-date
          :col-label    "Open date"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "ReasonEstablishmentClosed (code)"
          :col-name     :reason-establishment-closed-code
          :col-label    "Reason establishment closed (code)"}
         {:csv-col-name "ReasonEstablishmentClosed (name)"
          :col-name     :reason-establishment-closed-name
          :col-label    "Reason establishment closed"}
         {:csv-col-name "CloseDate"
          :col-name     :close-date
          :col-label    "Close date"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "PhaseOfEducation (code)"
          :col-name     :phase-of-education-code
          :col-label    "Phase of education (code)"}
         {:csv-col-name "PhaseOfEducation (name)"
          :col-name     :phase-of-education-name
          :col-label    "Phase of education"}
         {:csv-col-name "StatutoryLowAge"
          :col-name     :statutory-low-age
          :col-label    "Age range (low)"}
         {:csv-col-name "StatutoryHighAge"
          :col-name     :statutory-high-age
          :col-label    "Age range (high)"}
         {:csv-col-name "Boarders (code)"
          :col-name     :boarders-code
          :col-label    "Boarders (code)"}
         {:csv-col-name "Boarders (name)"
          :col-name     :boarders-name
          :col-label    "Boarders"}
         {:csv-col-name "NurseryProvision (name)"
          :col-name     :nursery-provision-name
          :col-label    "Nursery provision"}
         {:csv-col-name "OfficialSixthForm (code)"
          :col-name     :official-sixth-form-code
          :col-label    "Official sixth form (code)"}
         {:csv-col-name "OfficialSixthForm (name)"
          :col-name     :official-sixth-form-name
          :col-label    "Official sixth form"}
         {:csv-col-name "Gender (code)"
          :col-name     :gender-code
          :col-label    "Gender of entry (code)"}
         {:csv-col-name "Gender (name)"
          :col-name     :gender-name
          :col-label    "Gender of entry"}
         {:csv-col-name "ReligiousCharacter (code)"
          :col-name     :religious-character-code
          :col-label    "Religious character (code)"}
         {:csv-col-name "ReligiousCharacter (name)"
          :col-name     :religious-character-name
          :col-label    "Religious character"}
         {:csv-col-name "ReligiousEthos (name)"
          :col-name     :religious-ethos-name
          :col-label    "Religious ethos"}
         {:csv-col-name "Diocese (code)"
          :col-name     :diocese-code
          :col-label    "Diocese (code)"}
         {:csv-col-name "Diocese (name)"
          :col-name     :diocese-name
          :col-label    "Diocese"}
         {:csv-col-name "AdmissionsPolicy (code)"
          :col-name     :admissions-policy-code
          :col-label    "Admissons policy (code)"}
         {:csv-col-name "AdmissionsPolicy (name)"
          :col-name     :admissions-policy-name
          :col-label    "Admissons policy"}
         {:csv-col-name "SchoolCapacity"
          :col-name     :school-capacity
          :col-label    "School capacity"}
         {:csv-col-name "SpecialClasses (code)"
          :col-name     :special-classes-code
          :col-label    "Special classes (code)"}
         {:csv-col-name "SpecialClasses (name)"
          :col-name     :special-classes-name
          :col-label    "Special classes"}
         {:csv-col-name "CensusDate"
          :col-name     :school-census-date
          :col-label    "School census date"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "NumberOfPupils"
          :col-name     :number-of-pupils
          :col-label    "Number of pupils"}
         {:csv-col-name "NumberOfBoys"
          :col-name     :number-of-boys
          :col-label    "Number of boys"}
         {:csv-col-name "NumberOfGirls"
          :col-name     :number-of-girls
          :col-label    "Number of girls"}
         {:csv-col-name "PercentageFSM"
          :col-name     :percentage-fsm
          :col-label    "Percentage FSM"}
         {:csv-col-name "TrustSchoolFlag (code)"
          :col-name     :trust-school-flag-code
          :col-label    "Trust school flag (code)"}
         {:csv-col-name "TrustSchoolFlag (name)"
          :col-name     :trust-school-flag-name
          :col-label    "Trust school flag"}
         {:csv-col-name "Trusts (code)"
          :col-name     :trusts-code
          :col-label    "Academy trust or trust (code)"}
         {:csv-col-name "Trusts (name)"
          :col-name     :trusts-name
          :col-label    "Academy trust or trust"}
         {:csv-col-name "SchoolSponsorFlag (name)"
          :col-name     :school-sponsor-flag-name
          :col-label    "School sponsor flag"}
         {:csv-col-name "SchoolSponsors (name)"
          :col-name     :school-sponsors-name
          :col-label    "Academy sponsor"}
         {:csv-col-name "FederationFlag (name)"
          :col-name     :federation-flag-name
          :col-label    "Federation flag"}
         {:csv-col-name "Federations (code)"
          :col-name     :federations-code
          :col-label    "Federation (code)"}
         {:csv-col-name "Federations (name)"
          :col-name     :federations-name
          :col-label    "Federation"}
         {:csv-col-name "UKPRN"
          :col-name     :ukprn
          :col-label    "UK provider reference number (UKPRN)"
          :parser-fn    :string}
         {:csv-col-name "FEHEIdentifier"
          :col-name     :fehe-identifier
          :col-label    "FEHE identifier"
          :parser-fn    :string}
         {:csv-col-name "FurtherEducationType (name)"
          :col-name     :further-education-type-name
          :col-label    "Further education type"}
         {:csv-col-name "OfstedLastInsp"
          :col-name     :ofsted-last-insp
          :col-label    "Date of last OFSTED inspection"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "OfstedSpecialMeasures (code)"
          :col-name     :ofsted-special-measures-code
          :col-label    "OFSTED special measures (code)"}
         {:csv-col-name "OfstedSpecialMeasures (name)"
          :col-name     :ofsted-special-measures-name
          :col-label    "OFSTED special measures"}
         {:csv-col-name "LastChangedDate"
          :col-name     :last-changed-date
          :col-label    "Last Changed Date"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "Street"
          :col-name     :street
          :col-label    "Street"}
         {:csv-col-name "Locality"
          :col-name     :locality
          :col-label    "Locality"}
         {:csv-col-name "Address3"
          :col-name     :address3
          :col-label    "Address 3"}
         {:csv-col-name "Town"
          :col-name     :town
          :col-label    "Town"}
         {:csv-col-name "County (name)"
          :col-name     :county-name
          :col-label    "County"}
         {:csv-col-name "Postcode"
          :col-name     :postcode
          :col-label    "Postcode"}
         {:csv-col-name "SchoolWebsite"
          :col-name     :school-website
          :col-label    "Website"}
         {:csv-col-name "TelephoneNum"
          :col-name     :telephone-num
          :col-label    "Telephone"
          :parser-fn    :string}
         {:csv-col-name "HeadTitle (name)"
          :col-name     :head-title-name
          :col-label    "Headteacher/Principal title"}
         {:csv-col-name "HeadFirstName"
          :col-name     :head-first-name
          :col-label    "Headteacher/Principal first name"}
         {:csv-col-name "HeadLastName"
          :col-name     :head-last-name
          :col-label    "Headteacher/Principal last name"}
         {:csv-col-name "HeadPreferredJobTitle"
          :col-name     :head-preferred-job-title
          :col-label    "Headteacher/Principal preferred job title"}
         {:csv-col-name "BSOInspectorateName (name)"
          :col-name     :bso-inspectorate-name-name
          :col-label    "BSO inspectorate name"}
         {:csv-col-name "InspectorateReport"
          :col-name     :inspectorate-report
          :col-label    "Inspectorate report URL"}
         {:csv-col-name "DateOfLastInspectionVisit"
          :col-name     :date-of-last-inspection-visit
          :col-label    "Date of last inspection visit"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "NextInspectionVisit"
          :col-name     :next-inspection-visit
          :col-label    "Date of next inspection visit"
          :parser-fn    [:packed-local-date parse-date]}
         {:csv-col-name "TeenMoth (name)"
          :col-name     :teen-moth-name
          :col-label    "Teenage mothers"}
         {:csv-col-name "TeenMothPlaces"
          :col-name     :teen-moth-places
          :col-label    "Teenage mothers capacity"}
         {:csv-col-name "CCF (name)"
          :col-name     :ccf-name
          :col-label    "Child care facilities"}
         {:csv-col-name "SENPRU (name)"
          :col-name     :senpru-name
          :col-label    "PRU provision for SEN"}
         {:csv-col-name "EBD (name)"
          :col-name     :pru-ebd-name
          :col-label    "PRU provision for EBD"}
         {:csv-col-name "PlacesPRU"
          :col-name     :places-pru
          :col-label    "Number of PRU places"}
         {:csv-col-name "FTProv (name)"
          :col-name     :ft-prov-name
          :col-label    "PRU offer full time provision"}
         {:csv-col-name "EdByOther (name)"
          :col-name     :ed-by-other-name
          :col-label    "PRU offer tuition by anther provider"}
         {:csv-col-name "Section41Approved (name)"
          :col-name     :section41-approved-name
          :col-label    "Section 41 approved"}
         {:csv-col-name "SEN1 (name)"
          :col-name     :sen-provision-type-1
          :col-label    "Type of SEN provision 1"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN2 (name)"
          :col-name     :sen-provision-type-2
          :col-label    "Type of SEN provision 2"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN3 (name)"
          :col-name     :sen-provision-type-3
          :col-label    "Type of SEN provision 3"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN4 (name)"
          :col-name     :sen-provision-type-4
          :col-label    "Type of SEN provision 4"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN5 (name)"
          :col-name     :sen-provision-type-5
          :col-label    "Type of SEN provision 5"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN6 (name)"
          :col-name     :sen-provision-type-6
          :col-label    "Type of SEN provision 6"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN7 (name)"
          :col-name     :sen-provision-type-7
          :col-label    "Type of SEN provision 7"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN8 (name)"
          :col-name     :sen-provision-type-8
          :col-label    "Type of SEN provision 8"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN9 (name)"
          :col-name     :sen-provision-type-9
          :col-label    "Type of SEN provision 9"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN10 (name)"
          :col-name     :sen-provision-type-10
          :col-label    "Type of SEN provision 10"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN11 (name)"
          :col-name     :sen-provision-type-11
          :col-label    "Type of SEN provision 11"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN12 (name)"
          :col-name     :sen-provision-type-12
          :col-label    "Type of SEN provision 12"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "SEN13 (name)"
          :col-name     :sen-provision-type-13
          :col-label    "Type of SEN provision 13"
          :parser-fn    [:string parse-sen-provision-type-name]}
         {:csv-col-name "TypeOfResourcedProvision (name)"
          :col-name     :type-of-resourced-provision-name
          :col-label    "Type of resourced provision"}
         {:csv-col-name "ResourcedProvisionOnRoll"
          :col-name     :resourced-provision-on-roll
          :col-label    "Resourced provision number on roll"}
         {:csv-col-name "ResourcedProvisionCapacity"
          :col-name     :resourced-provision-capacity
          :col-label    "Resourced provision capacity"}
         {:csv-col-name "SenUnitOnRoll"
          :col-name     :sen-unit-on-roll
          :col-label    "SEN unit number on roll"}
         {:csv-col-name "SenUnitCapacity"
          :col-name     :sen-unit-capacity
          :col-label    "SEN unit capacity"}
         {:csv-col-name "GOR (code)"
          :col-name     :gor-code
          :col-label    "GOR (code)"}
         {:csv-col-name "GOR (name)"
          :col-name     :gor-name
          :col-label    "GOR"}
         {:csv-col-name "DistrictAdministrative (code)"
          :col-name     :district-administrative-code
          :col-label    "District administrative (code)"}
         {:csv-col-name "DistrictAdministrative (name)"
          :col-name     :district-administrative-name
          :col-label    "District administrative"}
         {:csv-col-name "AdministrativeWard (code)"
          :col-name     :administrative-ward-code
          :col-label    "Administrative ward (code)"}
         {:csv-col-name "AdministrativeWard (name)"
          :col-name     :administrative-ward-name
          :col-label    "Administrative ward"}
         {:csv-col-name "ParliamentaryConstituency (code)"
          :col-name     :parliamentary-constituency-code
          :col-label    "Parliamentary constituency (code)"}
         {:csv-col-name "ParliamentaryConstituency (name)"
          :col-name     :parliamentary-constituency-name
          :col-label    "Parliamentary constituency"}
         {:csv-col-name "UrbanRural (code)"
          :col-name     :urban-rural-code
          :col-label    "Urban rural (code)"}
         {:csv-col-name "UrbanRural (name)"
          :col-name     :urban-rural-name
          :col-label    "Urban rural"}
         {:csv-col-name "GSSLACode (name)"
          :col-name     :gssla-code-name
          :col-label    "GSSLA code"}
         {:csv-col-name "Easting"
          :col-name     :easting
          :col-label    "Easting"}
         {:csv-col-name "Northing"
          :col-name     :northing
          :col-label    "Northing"}
         {:csv-col-name "MSOA (name)"
          :col-name     :msoa-name
          :col-label    "MSOA"}
         {:csv-col-name "LSOA (name)"
          :col-name     :lsoa-name
          :col-label    "LSOA"}
         {:csv-col-name "InspectorateName (name)"
          :col-name     :inspectorate-name-name
          :col-label    "Inspectorate name"}
         {:csv-col-name "SENStat"
          :col-name     :sen-stat
          :col-label    "Number of special pupils under a SEN statement or EHCP"}
         {:csv-col-name "SENNoStat"
          :col-name     :sen-no-stat
          :col-label    "Number of special pupils not under a SEN statement or EHCP"}
         {:csv-col-name "BoardingEstablishment (name)"
          :col-name     :boarding-establishment-name
          :col-label    "Boarding establishment"}
         {:csv-col-name "PropsName"
          :col-name     :props-name
          :col-label    "Proprietor's name"}
         {:csv-col-name "PreviousLA (code)"
          :col-name     :previous-la-code
          :col-label    "Previous local authority (code)"
          :parser-fn    :string}
         {:csv-col-name "PreviousLA (name)"
          :col-name     :previous-la-name
          :col-label    "Previous local authority"}
         {:csv-col-name "PreviousEstablishmentNumber"
          :col-name     :previous-establishment-number
          :col-label    "Previous establishment number"
          :parser-fn    :string}
         {:csv-col-name "OfstedRating (name)"
          :col-name     :ofsted-rating-name
          :col-label    "OFSTED rating"}
         {:csv-col-name "RSCRegion (name)"
          :col-name     :rsc-region-name
          :col-label    "RSC region"}
         {:csv-col-name "Country (name)"
          :col-name     :country-name
          :col-label    "Country"}
         {:csv-col-name "UPRN"
          :col-name     :uprn
          :col-label    "UPRN"
          :parser-fn    :string}
         {:csv-col-name "SiteName"
          :col-name     :site-name
          :col-label    "Site name"}
         {:csv-col-name "QABName (code)"
          :col-name     :qab-name-code
          :col-label    "QAB name (code)"}
         {:csv-col-name "QABName (name)"
          :col-name     :qab-name-name
          :col-label    "QAB name"}
         {:csv-col-name "EstablishmentAccredited (code)"
          :col-name     :establishment-accredited-code
          :col-label    "Establishment accredited (code)"}
         {:csv-col-name "EstablishmentAccredited (name)"
          :col-name     :establishment-accredited-name
          :col-label    "Establishment accredited"}
         {:csv-col-name "QABReport"
          :col-name     :qab-report
          :col-label    "QAB report"
          :parser-fn    :string}
         {:csv-col-name "CHNumber"
          :col-name     :ch-number
          :col-label    "CH number"
          :parser-fn    :string}
         {:csv-col-name "MSOA (code)"
          :col-name     :msoa-code
          :col-label    "MSOA (code)"}
         {:csv-col-name "LSOA (code)"
          :col-name     :lsoa-code
          :col-label    "LSOA (code)"}
         {:csv-col-name "FSM"
          :col-name     :fsm
          :col-label    "FSM"}
         {:csv-col-name "AccreditationExpiryDate"
          :col-name     :accreditation-expiry-date
          :col-label    "Accreditation expiry date"
          :parser-fn    [:packed-local-date parse-date]}] $
    ;; Add CSV file column numbers (for ordering)
    (map-indexed (fn [idx m] (assoc m :csv-col-num (inc idx))) $)
    ;; Extract into a map with `:csv-col-name`s as keys
    (reduce (fn [m v] (assoc m (:csv-col-name v) v)) {} $)
    ;; Order the map
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :csv-col-num))) $)))

(comment
  ;; Keyword column names created from CSV column lables using:
  (let [csv-col-name->keyword-col-name (fn [s] (-> s
                                                   (clojure.string/replace #"(?<!^)([A-Z])(?=[a-z])" "-$1")
                                                   (clojure.string/replace #"(?<=[a-z])([A-Z])" "-$1")
                                                   (clojure.string/replace #"[()]" "")
                                                   (clojure.string/replace #" +" "-")
                                                   (clojure.string/lower-case)
                                                   keyword))]
    (map csv-col-name->keyword-col-name
         (keys edubaseall-csv-columns)))

  )

(def edubaseall-columns
  "Map (ordered) edubaseall dataset column names to (maps of) metadata."
  (as-> edubaseall-csv-columns $
    (update-keys $ (update-vals $ :col-name))
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :csv-col-num))) $)))


;;; ## Read establishment data
(def edubaseall-csv-parser-fn
  "Default parser-fn for ds/->dataset read of edubaseall with CSV column names."
  (as-> edubaseall-csv-columns $
    (filter (comp some? :parser-fn last) $)
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :csv-col-num)))
          (update-vals $ :parser-fn))))

(def edubaseall-csv-key-fn
  "Default key-fn to be applied to edubaseall CSV file column names to obtain dataset column names."
  #((update-vals edubaseall-csv-columns :col-name) % %))

(def edubaseall-parser-fn
  "Default parser-fn for ds/->dataset read of edubaseall CSV file after `key-fn` applied to CSV column names."
  (update-keys edubaseall-csv-parser-fn (update-vals edubaseall-csv-columns :col-name)))

(defn edubaseall->ds
  "Read GIAS edubaseall \"all establishment\" data from CSV file into a dataset.
   Use optional `options` map to specify:
   - CSV file to read: via `::edubaseall-file-path` or `::edubaseall-resource-file-name` (for files in resource folder).
     [Defaults to `::edubaseall-resource-file-name` of `default-edubaseall-resource-file-name`.]
   - Additional or over-riding options for `->dataset`."
  ([] (edubaseall->ds {}))
  ([{::keys [edubaseall-resource-file-name edubaseall-file-path]
     :or    {edubaseall-resource-file-name default-edubaseall-resource-file-name}
     :as    options}]
   (with-open [in (-> (or edubaseall-file-path (io/resource edubaseall-resource-file-name))
                      io/file
                      io/input-stream)]
     (ds/->dataset in (merge {:file-type    :csv
                              :separator    ","
                              :dataset-name (or edubaseall-file-path edubaseall-resource-file-name)
                              :header-row?  true
                              :key-fn       edubaseall-csv-key-fn
                              :parser-fn    edubaseall-parser-fn}
                             options)))))

(comment
  (defn- csv-ds-column-info
    "Column info for a dataset `ds` read from CSV file."
    [ds col-name->csv-col-name col-name->label]
    (-> ds
        (tc/info)
        (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
        (tc/map-columns :csv-col-name [:col-name] col-name->csv-col-name)
        (tc/map-columns :col-label    [:col-name] col-name->label)
        (tc/reorder-columns [:col-name :csv-col-name :col-label])))

  (-> (edubaseall->ds
       #_{::edubaseall-file-path "/tmp/edubasealldata20230421.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230421.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230817.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230918.csv"}
       )
      (csv-ds-column-info (update-vals edubaseall-columns :csv-col-name)
                          (update-vals edubaseall-columns :col-label))
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20240129.csv: descriptive-stats [140 8]:
  ;;    |                         :col-name |                    :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|----------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |                              URN |                                                        URN |            :string |    50686 |          0 |            |            |
  ;;    |                          :la-code |                        LA (code) |                                                  LA (code) |            :string |    50686 |          0 |            |            |
  ;;    |                          :la-name |                        LA (name) |                                                         LA |            :string |    50686 |          0 |            |            |
  ;;    |             :establishment-number |              EstablishmentNumber |                                       Establishment Number |            :string |    50551 |        135 |            |            |
  ;;    |               :establishment-name |                EstablishmentName |                                      School / College Name |            :string |    50686 |          0 |            |            |
  ;;    |       :type-of-establishment-code |       TypeOfEstablishment (code) |                                  Establishment type (code) |             :int16 |    50686 |          0 |      1.000 |      57.00 |
  ;;    |       :type-of-establishment-name |       TypeOfEstablishment (name) |                                         Establishment type |            :string |    50686 |          0 |            |            |
  ;;    |    :establishment-type-group-code |    EstablishmentTypeGroup (code) |                            Establishment type group (code) |             :int16 |    50686 |          0 |      1.000 |      13.00 |
  ;;    |    :establishment-type-group-name |    EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    50686 |          0 |            |            |
  ;;    |        :establishment-status-code |       EstablishmentStatus (code) |                                Establishment status (code) |             :int16 |    50686 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |       EstablishmentStatus (name) |                                       Establishment status |            :string |    50686 |          0 |            |            |
  ;;    | :reason-establishment-opened-code | ReasonEstablishmentOpened (code) |                         Reason establishment opened (code) |             :int16 |    50686 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-opened-name | ReasonEstablishmentOpened (name) |                                Reason establishment opened |            :string |    49993 |        693 |            |            |
  ;;    |                        :open-date |                         OpenDate |                                                  Open date | :packed-local-date |    20295 |      30391 | 1800-01-01 | 2024-08-01 |
  ;;    | :reason-establishment-closed-code | ReasonEstablishmentClosed (code) |                         Reason establishment closed (code) |             :int16 |    50686 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-closed-name | ReasonEstablishmentClosed (name) |                                Reason establishment closed |            :string |    38059 |      12627 |            |            |
  ;;    |                       :close-date |                        CloseDate |                                                 Close date | :packed-local-date |    23273 |      27413 | 1900-01-01 | 2026-08-31 |
  ;;    |          :phase-of-education-code |          PhaseOfEducation (code) |                                  Phase of education (code) |             :int16 |    50686 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |          PhaseOfEducation (name) |                                         Phase of education |            :string |    50686 |          0 |            |            |
  ;;    |                :statutory-low-age |                  StatutoryLowAge |                                            Age range (low) |             :int16 |    46726 |       3960 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |                 StatutoryHighAge |                                           Age range (high) |             :int16 |    46729 |       3957 |      3.000 |      99.00 |
  ;;    |                    :boarders-code |                  Boarders (code) |                                            Boarders (code) |             :int16 |    50686 |          0 |      0.000 |      9.000 |
  ;;    |                    :boarders-name |                  Boarders (name) |                                                   Boarders |            :string |    49053 |       1633 |            |            |
  ;;    |           :nursery-provision-name |          NurseryProvision (name) |                                          Nursery provision |            :string |    50655 |         31 |            |            |
  ;;    |         :official-sixth-form-code |         OfficialSixthForm (code) |                                 Official sixth form (code) |             :int16 |    50686 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |         OfficialSixthForm (name) |                                        Official sixth form |            :string |    50679 |          7 |            |            |
  ;;    |                      :gender-code |                    Gender (code) |                                     Gender of entry (code) |             :int16 |    50686 |          0 |      0.000 |      9.000 |
  ;;    |                      :gender-name |                    Gender (name) |                                            Gender of entry |            :string |    49274 |       1412 |            |            |
  ;;    |         :religious-character-code |        ReligiousCharacter (code) |                                 Religious character (code) |             :int16 |    50686 |          0 |      0.000 |      99.00 |
  ;;    |         :religious-character-name |        ReligiousCharacter (name) |                                        Religious character |            :string |    46976 |       3710 |            |            |
  ;;    |             :religious-ethos-name |            ReligiousEthos (name) |                                            Religious ethos |            :string |    47422 |       3264 |            |            |
  ;;    |                     :diocese-code |                   Diocese (code) |                                             Diocese (code) |            :string |    50686 |          0 |            |            |
  ;;    |                     :diocese-name |                   Diocese (name) |                                                    Diocese |            :string |    49353 |       1333 |            |            |
  ;;    |           :admissions-policy-code |          AdmissionsPolicy (code) |                                    Admissons policy (code) |             :int16 |    50686 |          0 |      0.000 |      9.000 |
  ;;    |           :admissions-policy-name |          AdmissionsPolicy (name) |                                           Admissons policy |            :string |    45430 |       5256 |            |            |
  ;;    |                  :school-capacity |                   SchoolCapacity |                                            School capacity |             :int16 |    38373 |      12313 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |            SpecialClasses (code) |                                     Special classes (code) |             :int16 |    50686 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |            SpecialClasses (name) |                                            Special classes |            :string |    50559 |        127 |            |            |
  ;;    |               :school-census-date |                       CensusDate |                                         School census date | :packed-local-date |    29058 |      21628 | 2017-01-19 | 2023-01-19 |
  ;;    |                 :number-of-pupils |                   NumberOfPupils |                                           Number of pupils |             :int16 |    29063 |      21623 |      0.000 |       3440 |
  ;;    |                   :number-of-boys |                     NumberOfBoys |                                             Number of boys |             :int16 |    29045 |      21641 |      0.000 |       1766 |
  ;;    |                  :number-of-girls |                    NumberOfGirls |                                            Number of girls |             :int16 |    29040 |      21646 |      0.000 |       1885 |
  ;;    |                   :percentage-fsm |                    PercentageFSM |                                             Percentage FSM |           :float64 |    28852 |      21834 |      0.000 |      100.0 |
  ;;    |           :trust-school-flag-code |           TrustSchoolFlag (code) |                                   Trust school flag (code) |             :int16 |    50686 |          0 |      0.000 |      5.000 |
  ;;    |           :trust-school-flag-name |           TrustSchoolFlag (name) |                                          Trust school flag |            :string |    50686 |          0 |            |            |
  ;;    |                      :trusts-code |                    Trusts (code) |                              Academy trust or trust (code) |             :int16 |    11196 |      39490 |       1028 |  1.772E+04 |
  ;;    |                      :trusts-name |                    Trusts (name) |                                     Academy trust or trust |            :string |    11196 |      39490 |            |            |
  ;;    |         :school-sponsor-flag-name |         SchoolSponsorFlag (name) |                                        School sponsor flag |            :string |    50686 |          0 |            |            |
  ;;    |             :school-sponsors-name |            SchoolSponsors (name) |                                            Academy sponsor |            :string |     8160 |      42526 |            |            |
  ;;    |             :federation-flag-name |            FederationFlag (name) |                                            Federation flag |            :string |    50686 |          0 |            |            |
  ;;    |                 :federations-code |               Federations (code) |                                          Federation (code) |            :string |     1128 |      49558 |            |            |
  ;;    |                 :federations-name |               Federations (name) |                                                 Federation |            :string |     1128 |      49558 |            |            |
  ;;    |                            :ukprn |                            UKPRN |                       UK provider reference number (UKPRN) |            :string |    32145 |      18541 |            |            |
  ;;    |                  :fehe-identifier |                   FEHEIdentifier |                                            FEHE identifier |            :string |      537 |      50149 |            |            |
  ;;    |      :further-education-type-name |      FurtherEducationType (name) |                                     Further education type |            :string |    46312 |       4374 |            |            |
  ;;    |                 :ofsted-last-insp |                   OfstedLastInsp |                             Date of last OFSTED inspection | :packed-local-date |    27692 |      22994 | 2006-05-12 | 2023-11-22 |
  ;;    |     :ofsted-special-measures-code |     OfstedSpecialMeasures (code) |                             OFSTED special measures (code) |             :int16 |    50686 |          0 |      0.000 |      0.000 |
  ;;    |     :ofsted-special-measures-name |     OfstedSpecialMeasures (name) |                                    OFSTED special measures |            :string |    50686 |          0 |            |            |
  ;;    |                :last-changed-date |                  LastChangedDate |                                          Last Changed Date | :packed-local-date |    50686 |          0 | 2013-10-24 | 2024-01-29 |
  ;;    |                           :street |                           Street |                                                     Street |            :string |    49493 |       1193 |            |            |
  ;;    |                         :locality |                         Locality |                                                   Locality |            :string |    27539 |      23147 |            |            |
  ;;    |                         :address3 |                         Address3 |                                                  Address 3 |            :string |     4104 |      46582 |            |            |
  ;;    |                             :town |                             Town |                                                       Town |            :string |    48639 |       2047 |            |            |
  ;;    |                      :county-name |                    County (name) |                                                     County |            :string |    38976 |      11710 |            |            |
  ;;    |                         :postcode |                         Postcode |                                                   Postcode |            :string |    49267 |       1419 |            |            |
  ;;    |                   :school-website |                    SchoolWebsite |                                                    Website |            :string |    24705 |      25981 |            |            |
  ;;    |                    :telephone-num |                     TelephoneNum |                                                  Telephone |            :string |    27437 |      23249 |            |            |
  ;;    |                  :head-title-name |                 HeadTitle (name) |                                Headteacher/Principal title |            :string |    41708 |       8978 |            |            |
  ;;    |                  :head-first-name |                    HeadFirstName |                           Headteacher/Principal first name |            :string |    43035 |       7651 |            |            |
  ;;    |                   :head-last-name |                     HeadLastName |                            Headteacher/Principal last name |            :string |    43093 |       7593 |            |            |
  ;;    |         :head-preferred-job-title |            HeadPreferredJobTitle |                  Headteacher/Principal preferred job title |            :string |    44736 |       5950 |            |            |
  ;;    |       :bso-inspectorate-name-name |       BSOInspectorateName (name) |                                      BSO inspectorate name |            :string |    50686 |          0 |            |            |
  ;;    |              :inspectorate-report |               InspectorateReport |                                    Inspectorate report URL |            :string |      249 |      50437 |            |            |
  ;;    |    :date-of-last-inspection-visit |        DateOfLastInspectionVisit |                              Date of last inspection visit | :packed-local-date |      254 |      50432 | 2016-03-03 | 2023-10-03 |
  ;;    |            :next-inspection-visit |              NextInspectionVisit |                              Date of next inspection visit | :packed-local-date |        0 |      50686 | 1970-01-01 | 1970-01-01 |
  ;;    |                   :teen-moth-name |                  TeenMoth (name) |                                            Teenage mothers |            :string |    50677 |          9 |            |            |
  ;;    |                 :teen-moth-places |                   TeenMothPlaces |                                   Teenage mothers capacity |             :int16 |       98 |      50588 |      0.000 |      45.00 |
  ;;    |                         :ccf-name |                       CCF (name) |                                      Child care facilities |            :string |    44838 |       5848 |            |            |
  ;;    |                      :senpru-name |                    SENPRU (name) |                                      PRU provision for SEN |            :string |    50675 |         11 |            |            |
  ;;    |                     :pru-ebd-name |                       EBD (name) |                                      PRU provision for EBD |            :string |    50679 |          7 |            |            |
  ;;    |                       :places-pru |                        PlacesPRU |                                       Number of PRU places |             :int16 |      603 |      50083 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |                    FTProv (name) |                              PRU offer full time provision |            :string |     1941 |      48745 |            |            |
  ;;    |                 :ed-by-other-name |                 EdByOther (name) |                       PRU offer tuition by anther provider |            :string |    44827 |       5859 |            |            |
  ;;    |          :section41-approved-name |         Section41Approved (name) |                                        Section 41 approved |            :string |    50686 |          0 |            |            |
  ;;    |             :sen-provision-type-1 |                      SEN1 (name) |                                    Type of SEN provision 1 |            :string |     5611 |      45075 |            |            |
  ;;    |             :sen-provision-type-2 |                      SEN2 (name) |                                    Type of SEN provision 2 |            :string |     2008 |      48678 |            |            |
  ;;    |             :sen-provision-type-3 |                      SEN3 (name) |                                    Type of SEN provision 3 |            :string |     1198 |      49488 |            |            |
  ;;    |             :sen-provision-type-4 |                      SEN4 (name) |                                    Type of SEN provision 4 |            :string |      838 |      49848 |            |            |
  ;;    |             :sen-provision-type-5 |                      SEN5 (name) |                                    Type of SEN provision 5 |            :string |      607 |      50079 |            |            |
  ;;    |             :sen-provision-type-6 |                      SEN6 (name) |                                    Type of SEN provision 6 |            :string |      531 |      50155 |            |            |
  ;;    |             :sen-provision-type-7 |                      SEN7 (name) |                                    Type of SEN provision 7 |            :string |      473 |      50213 |            |            |
  ;;    |             :sen-provision-type-8 |                      SEN8 (name) |                                    Type of SEN provision 8 |            :string |      399 |      50287 |            |            |
  ;;    |             :sen-provision-type-9 |                      SEN9 (name) |                                    Type of SEN provision 9 |            :string |      314 |      50372 |            |            |
  ;;    |            :sen-provision-type-10 |                     SEN10 (name) |                                   Type of SEN provision 10 |            :string |      218 |      50468 |            |            |
  ;;    |            :sen-provision-type-11 |                     SEN11 (name) |                                   Type of SEN provision 11 |            :string |      146 |      50540 |            |            |
  ;;    |            :sen-provision-type-12 |                     SEN12 (name) |                                   Type of SEN provision 12 |            :string |      102 |      50584 |            |            |
  ;;    |            :sen-provision-type-13 |                     SEN13 (name) |                                   Type of SEN provision 13 |            :string |        5 |      50681 |            |            |
  ;;    | :type-of-resourced-provision-name |  TypeOfResourcedProvision (name) |                                Type of resourced provision |            :string |     7209 |      43477 |            |            |
  ;;    |      :resourced-provision-on-roll |         ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     1961 |      48725 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |       ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     1994 |      48692 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |                    SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      901 |      49785 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |                  SenUnitCapacity |                                          SEN unit capacity |             :int16 |      920 |      49766 |      0.000 |      427.0 |
  ;;    |                         :gor-code |                       GOR (code) |                                                 GOR (code) |            :string |    50686 |          0 |            |            |
  ;;    |                         :gor-name |                       GOR (name) |                                                        GOR |            :string |    50686 |          0 |            |            |
  ;;    |     :district-administrative-code |    DistrictAdministrative (code) |                             District administrative (code) |            :string |    50686 |          0 |            |            |
  ;;    |     :district-administrative-name |    DistrictAdministrative (name) |                                    District administrative |            :string |    49267 |       1419 |            |            |
  ;;    |         :administrative-ward-code |        AdministrativeWard (code) |                                 Administrative ward (code) |            :string |    50686 |          0 |            |            |
  ;;    |         :administrative-ward-name |        AdministrativeWard (name) |                                        Administrative ward |            :string |    49268 |       1418 |            |            |
  ;;    |  :parliamentary-constituency-code | ParliamentaryConstituency (code) |                          Parliamentary constituency (code) |            :string |    50686 |          0 |            |            |
  ;;    |  :parliamentary-constituency-name | ParliamentaryConstituency (name) |                                 Parliamentary constituency |            :string |    49268 |       1418 |            |            |
  ;;    |                 :urban-rural-code |                UrbanRural (code) |                                         Urban rural (code) |            :string |    50686 |          0 |            |            |
  ;;    |                 :urban-rural-name |                UrbanRural (name) |                                                Urban rural |            :string |    49267 |       1419 |            |            |
  ;;    |                  :gssla-code-name |                 GSSLACode (name) |                                                 GSSLA code |            :string |    50686 |          0 |            |            |
  ;;    |                          :easting |                          Easting |                                                    Easting |             :int32 |    48939 |       1747 |      0.000 |  6.551E+05 |
  ;;    |                         :northing |                         Northing |                                                   Northing |             :int32 |    48939 |       1747 |      0.000 |  8.119E+05 |
  ;;    |                        :msoa-name |                      MSOA (name) |                                                       MSOA |            :string |    49268 |       1418 |            |            |
  ;;    |                        :lsoa-name |                      LSOA (name) |                                                       LSOA |            :string |    49267 |       1419 |            |            |
  ;;    |           :inspectorate-name-name |          InspectorateName (name) |                                          Inspectorate name |            :string |     4851 |      45835 |            |            |
  ;;    |                         :sen-stat |                          SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3737 |      46949 |      0.000 |      315.0 |
  ;;    |                      :sen-no-stat |                        SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3545 |      47141 |      0.000 |      476.0 |
  ;;    |      :boarding-establishment-name |     BoardingEstablishment (name) |                                     Boarding establishment |            :string |     2610 |      48076 |            |            |
  ;;    |                       :props-name |                        PropsName |                                          Proprietor's name |            :string |     3339 |      47347 |            |            |
  ;;    |                 :previous-la-code |                PreviousLA (code) |                            Previous local authority (code) |            :string |    50686 |          0 |            |            |
  ;;    |                 :previous-la-name |                PreviousLA (name) |                                   Previous local authority |            :string |    16648 |      34038 |            |            |
  ;;    |    :previous-establishment-number |      PreviousEstablishmentNumber |                              Previous establishment number |            :string |     8075 |      42611 |            |            |
  ;;    |               :ofsted-rating-name |              OfstedRating (name) |                                              OFSTED rating |            :string |    27603 |      23083 |            |            |
  ;;    |                  :rsc-region-name |                 RSCRegion (name) |                                                 RSC region |            :string |    47718 |       2968 |            |            |
  ;;    |                     :country-name |                   Country (name) |                                                    Country |            :string |     2937 |      47749 |            |            |
  ;;    |                             :uprn |                             UPRN |                                                       UPRN |            :string |    37994 |      12692 |            |            |
  ;;    |                        :site-name |                         SiteName |                                                  Site name |            :string |        3 |      50683 |            |            |
  ;;    |                    :qab-name-code |                   QABName (code) |                                            QAB name (code) |             :int16 |    50686 |          0 |      0.000 |      1.000 |
  ;;    |                    :qab-name-name |                   QABName (name) |                                                   QAB name |            :string |    50686 |          0 |            |            |
  ;;    |    :establishment-accredited-code |   EstablishmentAccredited (code) |                            Establishment accredited (code) |             :int16 |    50686 |          0 |      0.000 |      1.000 |
  ;;    |    :establishment-accredited-name |   EstablishmentAccredited (name) |                                   Establishment accredited |            :string |    50686 |          0 |            |            |
  ;;    |                       :qab-report |                        QABReport |                                                 QAB report |            :string |        0 |      50686 |            |            |
  ;;    |                        :ch-number |                         CHNumber |                                                  CH number |            :string |        0 |      50686 |            |            |
  ;;    |                        :msoa-code |                      MSOA (code) |                                                MSOA (code) |            :string |    50686 |          0 |            |            |
  ;;    |                        :lsoa-code |                      LSOA (code) |                                                LSOA (code) |            :string |    50686 |          0 |            |            |
  ;;    |                              :fsm |                              FSM |                                                        FSM |             :int16 |    28853 |      21833 |      0.000 |      925.0 |
  ;;    |        :accreditation-expiry-date |          AccreditationExpiryDate |                                  Accreditation expiry date | :packed-local-date |        2 |      50684 | 2026-10-26 | 2026-12-14 |

  )


(comment
  ;; Write distinct establishment types to data file
  (-> (edubaseall->ds {:column-allowlist (map (update-vals edubaseall-columns :csv-col-name) [:type-of-establishment-code    :type-of-establishment-name
                                                                                              :establishment-type-group-code :establishment-type-group-name])
                       :dataset-name     (str (re-find #".+(?=\.csv$)" default-edubaseall-resource-file-name) "-establishment-types" ".csv")})
      (tc/unique-by)
      (tc/order-by [:type-of-establishment-code])
      (as-> $
          (tc/write! $ (str "./data/" (tc/dataset-name $)))))
  )



;;; # edubaseall for SEND
(def edubaseall-send-columns
  (as-> [:urn
         :last-changed-date
         ;; Establishment
         :ukprn
         :establishment-number
         :establishment-name
         :type-of-establishment-name
         :establishment-type-group-name
         :la-code
         :la-name
         ;; Status
         :establishment-status-name
         :open-date
         :close-date
         ;; Phase & ages
         :phase-of-education-name
         :statutory-low-age
         :statutory-high-age
         :further-education-type-name
         ;; Overall capacity & NOR
         :school-census-date
         :school-capacity
         :number-of-pupils
         ;; PRU
         :places-pru
         :senpru-name
         ;; Special classes and #EHCPs
         :special-classes-name
         :sen-stat
         :sen-no-stat
         ;; RP & SENU Provision
         #_:type-of-resourced-provision-name
         :sen-unit?                         ; derived
         :sen-unit-capacity
         :sen-unit-on-roll
         :resourced-provision?              ; derived
         :resourced-provision-capacity
         :resourced-provision-on-roll
         ;; SEN provision types
         :sen-provision-types-vec           ; derived
         ] $
    ;; Add order and (for columns coming from CSV file) column details
    (map-indexed (fn [idx k] {k (merge {:col-idx idx
                                        :col-name k}
                                       (select-keys (edubaseall-columns k) [:csv-col-name :col-label]))}) $)
    (into {} $)
    ;; Add details for derived columns
    (merge-with merge $ {:sen-provision-types-vec {:derived?  true
                                                   :col-label "SEN Provision Types (derived)"}
                         :sen-unit?               {:derived?  true
                                                   :col-label "SEN Unit? (derived)"}
                         :resourced-provision?    {:derived?  true
                                                   :col-label "Resourced Provision? (derived)"}})
    ;; Order the map
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :col-idx))) $)))

(defn edubaseall-send->ds
  "Read SEND related columns from GIAS edubaseall \"all establishment\" data from CSV file into a dataset
     with default column names, with additional derived columns:
     - `:sen-provision-types-vec` - vector of (upper-case) SEN provision type abbreviations extracted from \"SEN1\"-\"SEN13\"
     - `:resourced-provision?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has RP.
     - `:sen-unit?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has a SENU.
     Use optional `options` map to specify:
     - CSV file to read: via `::edubaseall-file-path` or `::edubaseall-resource-file-name` (for files in resource folder).
       [Defaults to `::edubaseall-resource-file-name` of `default-edubaseall-resource-file-name`.]
     - Additional or over-riding options for `->dataset`
       (though note that any `:column-allowlist`, `:column-blocklist` or `:key-fn` will be ignored)."
  ([] (edubaseall-send->ds {}))
  ([options]
   (let [sen-provision-type-columns (map (comp keyword (partial format "sen-provision-type-%,d")) (range 1 14))
         columns-to-read            ((comp distinct concat)
                                     (keys edubaseall-send-columns)
                                     sen-provision-type-columns
                                     [:type-of-resourced-provision-name])
         csv-columns-to-read        (keep (update-vals edubaseall-columns :csv-col-name) columns-to-read)]
     (-> (edubaseall->ds (-> options
                             (dissoc :key-fn :column-blocklist)
                             (assoc :column-allowlist csv-columns-to-read)))
         ;; Parse `:type-of-resourced-provision-name` into separate booleans for RP & SENU
         (tc/map-columns :resourced-provision?
                         [:type-of-resourced-provision-name]
                         #({"Not applicable"                   false
                            "Resourced provision"              true
                            "Resourced provision and SEN unit" true
                            "SEN unit"                         false} % %))
         (tc/map-columns :sen-unit?
                         [:type-of-resourced-provision-name]
                         #({"Not applicable"                   false
                            "Resourced provision"              false
                            "Resourced provision and SEN unit" true
                            "SEN unit"                         true} % %))
         ;; Pack non-nil SEN provision type abbreviations into a vector
         (tc/map-columns :sen-provision-types-vec sen-provision-type-columns #(filterv some? %&))
         ;; Arrange dataset
         (tc/select-columns (keys edubaseall-send-columns))
         (as-> $ (tc/set-dataset-name $ (str (tc/dataset-name $) " (SEND columns)")))))))

(defn edubaseall-send->map
  "Read SEND related columns from GIAS edubaseall \"all establishment\" data from CSV file and return as a map keyed by URN
     with values maps containing column values keyed by column names, with additional derived keys:
     - `:sen-provision-types-vec` - vector of (upper-case) SEN provision type abbreviations extracted from \"SEN1\"-\"SEN13\"
     - `:resourced-provision?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has RP.
     - `:sen-unit?` - Boolean indicating if `:type-of-resourced-provision-name` indicates estab. has a SENU.
     Use optional `options` map to specify:
     - CSV file to read: via `::edubaseall-file-path` or `::edubaseall-resource-file-name` (for files in resource folder).
       [Defaults to `::edubaseall-resource-file-name` of `default-edubaseall-resource-file-name`.]
     - Additional or over-riding options for `->dataset`
       (though note that any `:column-allowlist`, `:column-blocklist` or `:key-fn` will be ignored)."
  ([] (edubaseall-send->map {}))
  ([options]
   (let [edubaseall-send-ds (edubaseall-send->ds options)]
     (zipmap (edubaseall-send-ds :urn)
             (tc/rows edubaseall-send-ds :as-maps)))))

(comment
  (-> (edubaseall-send->ds
       #_{::edubaseall-file-path "/tmp/edubasealldata20230421.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230421.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230817.csv"}
       #_{::edubaseall-resource-file-name "edubasealldata20230918.csv"}
       )
      (csv-ds-column-info (update-vals edubaseall-send-columns :csv-col-name)
                          (update-vals edubaseall-send-columns :col-label))
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20240129.csv (SEND columns): descriptive-stats [31 8]:
  ;;    |                      :col-name |                 :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |--------------------------------|-------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                           :urn |                           URN |                                                        URN |            :string |    50686 |          0 |            |            |
  ;;    |             :last-changed-date |               LastChangedDate |                                          Last Changed Date | :packed-local-date |    50686 |          0 | 2013-10-24 | 2024-01-29 |
  ;;    |                         :ukprn |                         UKPRN |                       UK provider reference number (UKPRN) |            :string |    32145 |      18541 |            |            |
  ;;    |          :establishment-number |           EstablishmentNumber |                                       Establishment Number |            :string |    50551 |        135 |            |            |
  ;;    |            :establishment-name |             EstablishmentName |                                      School / College Name |            :string |    50686 |          0 |            |            |
  ;;    |    :type-of-establishment-name |    TypeOfEstablishment (name) |                                         Establishment type |            :string |    50686 |          0 |            |            |
  ;;    | :establishment-type-group-name | EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    50686 |          0 |            |            |
  ;;    |                       :la-code |                     LA (code) |                                                  LA (code) |            :string |    50686 |          0 |            |            |
  ;;    |                       :la-name |                     LA (name) |                                                         LA |            :string |    50686 |          0 |            |            |
  ;;    |     :establishment-status-name |    EstablishmentStatus (name) |                                       Establishment status |            :string |    50686 |          0 |            |            |
  ;;    |                     :open-date |                      OpenDate |                                                  Open date | :packed-local-date |    20295 |      30391 | 1800-01-01 | 2024-08-01 |
  ;;    |                    :close-date |                     CloseDate |                                                 Close date | :packed-local-date |    23273 |      27413 | 1900-01-01 | 2026-08-31 |
  ;;    |       :phase-of-education-name |       PhaseOfEducation (name) |                                         Phase of education |            :string |    50686 |          0 |            |            |
  ;;    |             :statutory-low-age |               StatutoryLowAge |                                            Age range (low) |             :int16 |    46726 |       3960 |      0.000 |      19.00 |
  ;;    |            :statutory-high-age |              StatutoryHighAge |                                           Age range (high) |             :int16 |    46729 |       3957 |      3.000 |      99.00 |
  ;;    |   :further-education-type-name |   FurtherEducationType (name) |                                     Further education type |            :string |    46312 |       4374 |            |            |
  ;;    |            :school-census-date |                    CensusDate |                                         School census date | :packed-local-date |    29058 |      21628 | 2017-01-19 | 2023-01-19 |
  ;;    |               :school-capacity |                SchoolCapacity |                                            School capacity |             :int16 |    38373 |      12313 |      1.000 |  1.000E+04 |
  ;;    |              :number-of-pupils |                NumberOfPupils |                                           Number of pupils |             :int16 |    29063 |      21623 |      0.000 |       3440 |
  ;;    |                    :places-pru |                     PlacesPRU |                                       Number of PRU places |             :int16 |      603 |      50083 |      0.000 |      300.0 |
  ;;    |                   :senpru-name |                 SENPRU (name) |                                      PRU provision for SEN |            :string |    50675 |         11 |            |            |
  ;;    |          :special-classes-name |         SpecialClasses (name) |                                            Special classes |            :string |    50559 |        127 |            |            |
  ;;    |                      :sen-stat |                       SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3737 |      46949 |      0.000 |      315.0 |
  ;;    |                   :sen-no-stat |                     SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3545 |      47141 |      0.000 |      476.0 |
  ;;    |                     :sen-unit? |                               |                                        SEN Unit? (derived) |           :boolean |     7209 |      43477 |            |            |
  ;;    |             :sen-unit-capacity |               SenUnitCapacity |                                          SEN unit capacity |             :int16 |      920 |      49766 |      0.000 |      427.0 |
  ;;    |              :sen-unit-on-roll |                 SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      901 |      49785 |      0.000 |      427.0 |
  ;;    |          :resourced-provision? |                               |                             Resourced Provision? (derived) |           :boolean |     7209 |      43477 |            |            |
  ;;    |  :resourced-provision-capacity |    ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     1994 |      48692 |      0.000 |       1250 |
  ;;    |   :resourced-provision-on-roll |      ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     1961 |      48725 |      0.000 |       1872 |
  ;;    |       :sen-provision-types-vec |                               |                              SEN Provision Types (derived) | :persistent-vector |    50686 |          0 |            |            |

  )

