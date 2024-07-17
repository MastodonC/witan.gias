(ns witan.gias
  "Read in \"Get Information About Schools\" (GIAS) all establishment data from CSV file downloaded from [get-information-schools.service.gov.uk/Downloads](https://www.get-information-schools.service.gov.uk/Downloads)"
  (:require [clojure.java.io :as io]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]))



;;; # Data files
(def default-edubaseall-resource-file-name
  "Name of default edubaseall resource file containing all establishment data"
  "edubasealldata20240717.csv")



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

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
          :parser-fn    [:local-date "dd-MM-uuuu"]}
         {:csv-col-name "ReasonEstablishmentClosed (code)"
          :col-name     :reason-establishment-closed-code
          :col-label    "Reason establishment closed (code)"}
         {:csv-col-name "ReasonEstablishmentClosed (name)"
          :col-name     :reason-establishment-closed-name
          :col-label    "Reason establishment closed"}
         {:csv-col-name "CloseDate"
          :col-name     :close-date
          :col-label    "Close date"
          :parser-fn    [:local-date "dd-MM-uuuu"]}
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
          :parser-fn    [:local-date "dd-MM-uuuu"]}
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
          :parser-fn    [:local-date "dd-MM-uuuu"]}
         {:csv-col-name "OfstedSpecialMeasures (code)"
          :col-name     :ofsted-special-measures-code
          :col-label    "OFSTED special measures (code)"}
         {:csv-col-name "OfstedSpecialMeasures (name)"
          :col-name     :ofsted-special-measures-name
          :col-label    "OFSTED special measures"}
         {:csv-col-name "LastChangedDate"
          :col-name     :last-changed-date
          :col-label    "Last Changed Date"
          :parser-fn    [:local-date "dd-MM-uuuu"]}
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
          :parser-fn    [:local-date "dd-MM-uuuu"]}
         {:csv-col-name "NextInspectionVisit"
          :col-name     :next-inspection-visit
          :col-label    "Date of next inspection visit"
          :parser-fn    [:local-date "dd-MM-uuuu"]}
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
         {:csv-col-name "RSCRegion (name)" ; Not in file downloaded 2024-07-17
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
          :parser-fn    [:local-date "dd-MM-uuuu"]}] $
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
       #_{::edubaseall-resource-file-name "edubasealldata20240717.csv"}
       )
      (csv-ds-column-info (update-vals edubaseall-columns :csv-col-name)
                          (update-vals edubaseall-columns :col-label))
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20240717.csv: descriptive-stats [139 8]:
  ;;    |                         :col-name |                    :csv-col-name |                                                 :col-label |   :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|----------------------------------|------------------------------------------------------------|-------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |                              URN |                                                        URN |     :string |    51090 |          0 |            |            |
  ;;    |                          :la-code |                        LA (code) |                                                  LA (code) |     :string |    51090 |          0 |            |            |
  ;;    |                          :la-name |                        LA (name) |                                                         LA |     :string |    51090 |          0 |            |            |
  ;;    |             :establishment-number |              EstablishmentNumber |                                       Establishment Number |     :string |    50955 |        135 |            |            |
  ;;    |               :establishment-name |                EstablishmentName |                                      School / College Name |     :string |    51090 |          0 |            |            |
  ;;    |       :type-of-establishment-code |       TypeOfEstablishment (code) |                                  Establishment type (code) |      :int16 |    51090 |          0 |      1.000 |      57.00 |
  ;;    |       :type-of-establishment-name |       TypeOfEstablishment (name) |                                         Establishment type |     :string |    51090 |          0 |            |            |
  ;;    |    :establishment-type-group-code |    EstablishmentTypeGroup (code) |                            Establishment type group (code) |      :int16 |    51090 |          0 |      1.000 |      13.00 |
  ;;    |    :establishment-type-group-name |    EstablishmentTypeGroup (name) |                                   Establishment type group |     :string |    51090 |          0 |            |            |
  ;;    |        :establishment-status-code |       EstablishmentStatus (code) |                                Establishment status (code) |      :int16 |    51090 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |       EstablishmentStatus (name) |                                       Establishment status |     :string |    51090 |          0 |            |            |
  ;;    | :reason-establishment-opened-code | ReasonEstablishmentOpened (code) |                         Reason establishment opened (code) |      :int16 |    51090 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-opened-name | ReasonEstablishmentOpened (name) |                                Reason establishment opened |     :string |    50364 |        726 |            |            |
  ;;    |                        :open-date |                         OpenDate |                                                  Open date | :local-date |    20789 |      30301 | 1800-01-01 | 2025-04-01 |
  ;;    | :reason-establishment-closed-code | ReasonEstablishmentClosed (code) |                         Reason establishment closed (code) |      :int16 |    51090 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-closed-name | ReasonEstablishmentClosed (name) |                                Reason establishment closed |     :string |    38016 |      13074 |            |            |
  ;;    |                       :close-date |                        CloseDate |                                                 Close date | :local-date |    23633 |      27457 | 1900-01-01 | 2034-08-31 |
  ;;    |          :phase-of-education-code |          PhaseOfEducation (code) |                                  Phase of education (code) |      :int16 |    51090 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |          PhaseOfEducation (name) |                                         Phase of education |     :string |    51090 |          0 |            |            |
  ;;    |                :statutory-low-age |                  StatutoryLowAge |                                            Age range (low) |      :int16 |    47126 |       3964 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |                 StatutoryHighAge |                                           Age range (high) |      :int16 |    47129 |       3961 |      3.000 |      99.00 |
  ;;    |                    :boarders-code |                  Boarders (code) |                                            Boarders (code) |      :int16 |    51090 |          0 |      0.000 |      9.000 |
  ;;    |                    :boarders-name |                  Boarders (name) |                                                   Boarders |     :string |    49406 |       1684 |            |            |
  ;;    |           :nursery-provision-name |          NurseryProvision (name) |                                          Nursery provision |     :string |    51055 |         35 |            |            |
  ;;    |         :official-sixth-form-code |         OfficialSixthForm (code) |                                 Official sixth form (code) |      :int16 |    51090 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |         OfficialSixthForm (name) |                                        Official sixth form |     :string |    51084 |          6 |            |            |
  ;;    |                      :gender-code |                    Gender (code) |                                     Gender of entry (code) |      :int16 |    51090 |          0 |      0.000 |      9.000 |
  ;;    |                      :gender-name |                    Gender (name) |                                            Gender of entry |     :string |    49671 |       1419 |            |            |
  ;;    |         :religious-character-code |        ReligiousCharacter (code) |                                 Religious character (code) |      :int16 |    51090 |          0 |      0.000 |      99.00 |
  ;;    |         :religious-character-name |        ReligiousCharacter (name) |                                        Religious character |     :string |    47318 |       3772 |            |            |
  ;;    |             :religious-ethos-name |            ReligiousEthos (name) |                                            Religious ethos |     :string |    47825 |       3265 |            |            |
  ;;    |                     :diocese-code |                   Diocese (code) |                                             Diocese (code) |     :string |    51090 |          0 |            |            |
  ;;    |                     :diocese-name |                   Diocese (name) |                                                    Diocese |     :string |    49732 |       1358 |            |            |
  ;;    |           :admissions-policy-code |          AdmissionsPolicy (code) |                                    Admissons policy (code) |      :int16 |    51090 |          0 |      0.000 |      9.000 |
  ;;    |           :admissions-policy-name |          AdmissionsPolicy (name) |                                           Admissons policy |     :string |    45739 |       5351 |            |            |
  ;;    |                  :school-capacity |                   SchoolCapacity |                                            School capacity |      :int16 |    38751 |      12339 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |            SpecialClasses (code) |                                     Special classes (code) |      :int16 |    51090 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |            SpecialClasses (name) |                                            Special classes |     :string |    50959 |        131 |            |            |
  ;;    |               :school-census-date |                       CensusDate |                                         School census date | :local-date |    29673 |      21417 | 2017-01-19 | 2024-01-18 |
  ;;    |                 :number-of-pupils |                   NumberOfPupils |                                           Number of pupils |      :int16 |    29671 |      21419 |      0.000 |       3467 |
  ;;    |                   :number-of-boys |                     NumberOfBoys |                                             Number of boys |      :int16 |    29660 |      21430 |      0.000 |       1812 |
  ;;    |                  :number-of-girls |                    NumberOfGirls |                                            Number of girls |      :int16 |    29655 |      21435 |      0.000 |       1917 |
  ;;    |                   :percentage-fsm |                    PercentageFSM |                                             Percentage FSM |    :float64 |    27035 |      24055 |      0.000 |      100.0 |
  ;;    |           :trust-school-flag-code |           TrustSchoolFlag (code) |                                   Trust school flag (code) |      :int16 |    51090 |          0 |      0.000 |      5.000 |
  ;;    |           :trust-school-flag-name |           TrustSchoolFlag (name) |                                          Trust school flag |     :string |    51090 |          0 |            |            |
  ;;    |                      :trusts-code |                    Trusts (code) |                              Academy trust or trust (code) |      :int16 |    11466 |      39624 |       1028 |  1.777E+04 |
  ;;    |                      :trusts-name |                    Trusts (name) |                                     Academy trust or trust |     :string |    11466 |      39624 |            |            |
  ;;    |         :school-sponsor-flag-name |         SchoolSponsorFlag (name) |                                        School sponsor flag |     :string |    51090 |          0 |            |            |
  ;;    |             :school-sponsors-name |            SchoolSponsors (name) |                                            Academy sponsor |     :string |     8406 |      42684 |            |            |
  ;;    |             :federation-flag-name |            FederationFlag (name) |                                            Federation flag |     :string |    51090 |          0 |            |            |
  ;;    |                 :federations-code |               Federations (code) |                                          Federation (code) |     :string |     1113 |      49977 |            |            |
  ;;    |                 :federations-name |               Federations (name) |                                                 Federation |     :string |     1113 |      49977 |            |            |
  ;;    |                            :ukprn |                            UKPRN |                       UK provider reference number (UKPRN) |     :string |    32517 |      18573 |            |            |
  ;;    |                  :fehe-identifier |                   FEHEIdentifier |                                            FEHE identifier |     :string |      537 |      50553 |            |            |
  ;;    |      :further-education-type-name |      FurtherEducationType (name) |                                     Further education type |     :string |    46313 |       4777 |            |            |
  ;;    |                 :ofsted-last-insp |                   OfstedLastInsp |                             Date of last OFSTED inspection | :local-date |    28221 |      22869 | 2006-05-12 | 2024-04-10 |
  ;;    |     :ofsted-special-measures-code |     OfstedSpecialMeasures (code) |                             OFSTED special measures (code) |      :int16 |    51090 |          0 |      0.000 |      0.000 |
  ;;    |     :ofsted-special-measures-name |     OfstedSpecialMeasures (name) |                                    OFSTED special measures |     :string |    51090 |          0 |            |            |
  ;;    |                :last-changed-date |                  LastChangedDate |                                          Last Changed Date | :local-date |    51090 |          0 | 2013-10-24 | 2024-07-17 |
  ;;    |                           :street |                           Street |                                                     Street |     :string |    49896 |       1194 |            |            |
  ;;    |                         :locality |                         Locality |                                                   Locality |     :string |    27776 |      23314 |            |            |
  ;;    |                         :address3 |                         Address3 |                                                  Address 3 |     :string |     4161 |      46929 |            |            |
  ;;    |                             :town |                             Town |                                                       Town |     :string |    49062 |       2028 |            |            |
  ;;    |                      :county-name |                    County (name) |                                                     County |     :string |    39313 |      11777 |            |            |
  ;;    |                         :postcode |                         Postcode |                                                   Postcode |     :string |    49644 |       1446 |            |            |
  ;;    |                   :school-website |                    SchoolWebsite |                                                    Website |     :string |    24721 |      26369 |            |            |
  ;;    |                    :telephone-num |                     TelephoneNum |                                                  Telephone |     :string |    27409 |      23681 |            |            |
  ;;    |                  :head-title-name |                 HeadTitle (name) |                                Headteacher/Principal title |     :string |    42128 |       8962 |            |            |
  ;;    |                  :head-first-name |                    HeadFirstName |                           Headteacher/Principal first name |     :string |    43432 |       7658 |            |            |
  ;;    |                   :head-last-name |                     HeadLastName |                            Headteacher/Principal last name |     :string |    43494 |       7596 |            |            |
  ;;    |         :head-preferred-job-title |            HeadPreferredJobTitle |                  Headteacher/Principal preferred job title |     :string |    44943 |       6147 |            |            |
  ;;    |       :bso-inspectorate-name-name |       BSOInspectorateName (name) |                                      BSO inspectorate name |     :string |    51090 |          0 |            |            |
  ;;    |              :inspectorate-report |               InspectorateReport |                                    Inspectorate report URL |     :string |      275 |      50815 |            |            |
  ;;    |    :date-of-last-inspection-visit |        DateOfLastInspectionVisit |                              Date of last inspection visit | :local-date |      278 |      50812 | 2016-03-03 | 2024-05-14 |
  ;;    |            :next-inspection-visit |              NextInspectionVisit |                              Date of next inspection visit | :local-date |        0 |      51090 | 1970-01-01 | 1970-01-01 |
  ;;    |                   :teen-moth-name |                  TeenMoth (name) |                                            Teenage mothers |     :string |    51080 |         10 |            |            |
  ;;    |                 :teen-moth-places |                   TeenMothPlaces |                                   Teenage mothers capacity |      :int16 |       97 |      50993 |      0.000 |      45.00 |
  ;;    |                         :ccf-name |                       CCF (name) |                                      Child care facilities |     :string |    45050 |       6040 |            |            |
  ;;    |                      :senpru-name |                    SENPRU (name) |                                      PRU provision for SEN |     :string |    51079 |         11 |            |            |
  ;;    |                     :pru-ebd-name |                       EBD (name) |                                      PRU provision for EBD |     :string |    51083 |          7 |            |            |
  ;;    |                       :places-pru |                        PlacesPRU |                                       Number of PRU places |      :int16 |      603 |      50487 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |                    FTProv (name) |                              PRU offer full time provision |     :string |     2153 |      48937 |            |            |
  ;;    |                 :ed-by-other-name |                 EdByOther (name) |                       PRU offer tuition by anther provider |     :string |    45039 |       6051 |            |            |
  ;;    |          :section41-approved-name |         Section41Approved (name) |                                        Section 41 approved |     :string |    51090 |          0 |            |            |
  ;;    |             :sen-provision-type-1 |                      SEN1 (name) |                                    Type of SEN provision 1 |     :string |     5675 |      45415 |            |            |
  ;;    |             :sen-provision-type-2 |                      SEN2 (name) |                                    Type of SEN provision 2 |     :string |     2037 |      49053 |            |            |
  ;;    |             :sen-provision-type-3 |                      SEN3 (name) |                                    Type of SEN provision 3 |     :string |     1202 |      49888 |            |            |
  ;;    |             :sen-provision-type-4 |                      SEN4 (name) |                                    Type of SEN provision 4 |     :string |      845 |      50245 |            |            |
  ;;    |             :sen-provision-type-5 |                      SEN5 (name) |                                    Type of SEN provision 5 |     :string |      621 |      50469 |            |            |
  ;;    |             :sen-provision-type-6 |                      SEN6 (name) |                                    Type of SEN provision 6 |     :string |      541 |      50549 |            |            |
  ;;    |             :sen-provision-type-7 |                      SEN7 (name) |                                    Type of SEN provision 7 |     :string |      476 |      50614 |            |            |
  ;;    |             :sen-provision-type-8 |                      SEN8 (name) |                                    Type of SEN provision 8 |     :string |      407 |      50683 |            |            |
  ;;    |             :sen-provision-type-9 |                      SEN9 (name) |                                    Type of SEN provision 9 |     :string |      321 |      50769 |            |            |
  ;;    |            :sen-provision-type-10 |                     SEN10 (name) |                                   Type of SEN provision 10 |     :string |      221 |      50869 |            |            |
  ;;    |            :sen-provision-type-11 |                     SEN11 (name) |                                   Type of SEN provision 11 |     :string |      145 |      50945 |            |            |
  ;;    |            :sen-provision-type-12 |                     SEN12 (name) |                                   Type of SEN provision 12 |     :string |       96 |      50994 |            |            |
  ;;    |            :sen-provision-type-13 |                     SEN13 (name) |                                   Type of SEN provision 13 |     :string |        3 |      51087 |            |            |
  ;;    | :type-of-resourced-provision-name |  TypeOfResourcedProvision (name) |                                Type of resourced provision |     :string |     7372 |      43718 |            |            |
  ;;    |      :resourced-provision-on-roll |         ResourcedProvisionOnRoll |                         Resourced provision number on roll |      :int16 |     2008 |      49082 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |       ResourcedProvisionCapacity |                               Resourced provision capacity |      :int16 |     2040 |      49050 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |                    SenUnitOnRoll |                                    SEN unit number on roll |      :int16 |      938 |      50152 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |                  SenUnitCapacity |                                          SEN unit capacity |      :int16 |      956 |      50134 |      0.000 |      427.0 |
  ;;    |                         :gor-code |                       GOR (code) |                                                 GOR (code) |     :string |    51090 |          0 |            |            |
  ;;    |                         :gor-name |                       GOR (name) |                                                        GOR |     :string |    51090 |          0 |            |            |
  ;;    |     :district-administrative-code |    DistrictAdministrative (code) |                             District administrative (code) |     :string |    51090 |          0 |            |            |
  ;;    |     :district-administrative-name |    DistrictAdministrative (name) |                                    District administrative |     :string |    49636 |       1454 |            |            |
  ;;    |         :administrative-ward-code |        AdministrativeWard (code) |                                 Administrative ward (code) |     :string |    51090 |          0 |            |            |
  ;;    |         :administrative-ward-name |        AdministrativeWard (name) |                                        Administrative ward |     :string |    49637 |       1453 |            |            |
  ;;    |  :parliamentary-constituency-code | ParliamentaryConstituency (code) |                          Parliamentary constituency (code) |     :string |    51090 |          0 |            |            |
  ;;    |  :parliamentary-constituency-name | ParliamentaryConstituency (name) |                                 Parliamentary constituency |     :string |    49637 |       1453 |            |            |
  ;;    |                 :urban-rural-code |                UrbanRural (code) |                                         Urban rural (code) |     :string |    51090 |          0 |            |            |
  ;;    |                 :urban-rural-name |                UrbanRural (name) |                                                Urban rural |     :string |    49636 |       1454 |            |            |
  ;;    |                  :gssla-code-name |                 GSSLACode (name) |                                                 GSSLA code |     :string |    51090 |          0 |            |            |
  ;;    |                          :easting |                          Easting |                                                    Easting |      :int32 |    49263 |       1827 |      0.000 |  6.551E+05 |
  ;;    |                         :northing |                         Northing |                                                   Northing |      :int32 |    49263 |       1827 |      0.000 |  8.119E+05 |
  ;;    |                        :msoa-name |                      MSOA (name) |                                                       MSOA |     :string |    49637 |       1453 |            |            |
  ;;    |                        :lsoa-name |                      LSOA (name) |                                                       LSOA |     :string |    49635 |       1455 |            |            |
  ;;    |           :inspectorate-name-name |          InspectorateName (name) |                                          Inspectorate name |     :string |     5110 |      45980 |            |            |
  ;;    |                         :sen-stat |                          SENStat |     Number of special pupils under a SEN statement or EHCP |      :int16 |     3760 |      47330 |      0.000 |      315.0 |
  ;;    |                      :sen-no-stat |                        SENNoStat | Number of special pupils not under a SEN statement or EHCP |      :int16 |     3559 |      47531 |      0.000 |      476.0 |
  ;;    |      :boarding-establishment-name |     BoardingEstablishment (name) |                                     Boarding establishment |     :string |     2612 |      48478 |            |            |
  ;;    |                       :props-name |                        PropsName |                                          Proprietor's name |     :string |     3466 |      47624 |            |            |
  ;;    |                 :previous-la-code |                PreviousLA (code) |                            Previous local authority (code) |     :string |    51090 |          0 |            |            |
  ;;    |                 :previous-la-name |                PreviousLA (name) |                                   Previous local authority |     :string |    16755 |      34335 |            |            |
  ;;    |    :previous-establishment-number |      PreviousEstablishmentNumber |                              Previous establishment number |     :string |     8130 |      42960 |            |            |
  ;;    |               :ofsted-rating-name |              OfstedRating (name) |                                              OFSTED rating |     :string |    28098 |      22992 |            |            |
  ;;    |                     :country-name |                   Country (name) |                                                    Country |     :string |     3164 |      47926 |            |            |
  ;;    |                             :uprn |                             UPRN |                                                       UPRN |     :string |    38370 |      12720 |            |            |
  ;;    |                        :site-name |                         SiteName |                                                  Site name |     :string |        3 |      51087 |            |            |
  ;;    |                    :qab-name-code |                   QABName (code) |                                            QAB name (code) |      :int16 |    51090 |          0 |      0.000 |      1.000 |
  ;;    |                    :qab-name-name |                   QABName (name) |                                                   QAB name |     :string |    51090 |          0 |            |            |
  ;;    |    :establishment-accredited-code |   EstablishmentAccredited (code) |                            Establishment accredited (code) |      :int16 |    51090 |          0 |      0.000 |      1.000 |
  ;;    |    :establishment-accredited-name |   EstablishmentAccredited (name) |                                   Establishment accredited |     :string |    51090 |          0 |            |            |
  ;;    |                       :qab-report |                        QABReport |                                                 QAB report |     :string |        1 |      51089 |            |            |
  ;;    |                        :ch-number |                         CHNumber |                                                  CH number |     :string |        3 |      51087 |            |            |
  ;;    |                        :msoa-code |                      MSOA (code) |                                                MSOA (code) |     :string |    51090 |          0 |            |            |
  ;;    |                        :lsoa-code |                      LSOA (code) |                                                LSOA (code) |     :string |    51090 |          0 |            |            |
  ;;    |                              :fsm |                              FSM |                                                        FSM |      :int16 |    27036 |      24054 |      0.000 |      940.0 |
  ;;    |        :accreditation-expiry-date |          AccreditationExpiryDate |                                  Accreditation expiry date | :local-date |        5 |      51085 | 2026-10-26 | 2027-05-23 |

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
       #_{::edubaseall-resource-file-name "edubasealldata20240717.csv"}
       )
      (csv-ds-column-info (update-vals edubaseall-send-columns :csv-col-name)
                          (update-vals edubaseall-send-columns :col-label))
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20240717.csv (SEND columns): descriptive-stats [31 8]:
  ;;    |                      :col-name |                 :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |--------------------------------|-------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                           :urn |                           URN |                                                        URN |            :string |    51090 |          0 |            |            |
  ;;    |             :last-changed-date |               LastChangedDate |                                          Last Changed Date |        :local-date |    51090 |          0 | 2013-10-24 | 2024-07-17 |
  ;;    |                         :ukprn |                         UKPRN |                       UK provider reference number (UKPRN) |            :string |    32517 |      18573 |            |            |
  ;;    |          :establishment-number |           EstablishmentNumber |                                       Establishment Number |            :string |    50955 |        135 |            |            |
  ;;    |            :establishment-name |             EstablishmentName |                                      School / College Name |            :string |    51090 |          0 |            |            |
  ;;    |    :type-of-establishment-name |    TypeOfEstablishment (name) |                                         Establishment type |            :string |    51090 |          0 |            |            |
  ;;    | :establishment-type-group-name | EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    51090 |          0 |            |            |
  ;;    |                       :la-code |                     LA (code) |                                                  LA (code) |            :string |    51090 |          0 |            |            |
  ;;    |                       :la-name |                     LA (name) |                                                         LA |            :string |    51090 |          0 |            |            |
  ;;    |     :establishment-status-name |    EstablishmentStatus (name) |                                       Establishment status |            :string |    51090 |          0 |            |            |
  ;;    |                     :open-date |                      OpenDate |                                                  Open date |        :local-date |    20789 |      30301 | 1800-01-01 | 2025-04-01 |
  ;;    |                    :close-date |                     CloseDate |                                                 Close date |        :local-date |    23633 |      27457 | 1900-01-01 | 2034-08-31 |
  ;;    |       :phase-of-education-name |       PhaseOfEducation (name) |                                         Phase of education |            :string |    51090 |          0 |            |            |
  ;;    |             :statutory-low-age |               StatutoryLowAge |                                            Age range (low) |             :int16 |    47126 |       3964 |      0.000 |      19.00 |
  ;;    |            :statutory-high-age |              StatutoryHighAge |                                           Age range (high) |             :int16 |    47129 |       3961 |      3.000 |      99.00 |
  ;;    |   :further-education-type-name |   FurtherEducationType (name) |                                     Further education type |            :string |    46313 |       4777 |            |            |
  ;;    |            :school-census-date |                    CensusDate |                                         School census date |        :local-date |    29673 |      21417 | 2017-01-19 | 2024-01-18 |
  ;;    |               :school-capacity |                SchoolCapacity |                                            School capacity |             :int16 |    38751 |      12339 |      1.000 |  1.000E+04 |
  ;;    |              :number-of-pupils |                NumberOfPupils |                                           Number of pupils |             :int16 |    29671 |      21419 |      0.000 |       3467 |
  ;;    |                    :places-pru |                     PlacesPRU |                                       Number of PRU places |             :int16 |      603 |      50487 |      0.000 |      300.0 |
  ;;    |                   :senpru-name |                 SENPRU (name) |                                      PRU provision for SEN |            :string |    51079 |         11 |            |            |
  ;;    |          :special-classes-name |         SpecialClasses (name) |                                            Special classes |            :string |    50959 |        131 |            |            |
  ;;    |                      :sen-stat |                       SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3760 |      47330 |      0.000 |      315.0 |
  ;;    |                   :sen-no-stat |                     SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3559 |      47531 |      0.000 |      476.0 |
  ;;    |                     :sen-unit? |                               |                                        SEN Unit? (derived) |           :boolean |     7372 |      43718 |            |            |
  ;;    |             :sen-unit-capacity |               SenUnitCapacity |                                          SEN unit capacity |             :int16 |      956 |      50134 |      0.000 |      427.0 |
  ;;    |              :sen-unit-on-roll |                 SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      938 |      50152 |      0.000 |      427.0 |
  ;;    |          :resourced-provision? |                               |                             Resourced Provision? (derived) |           :boolean |     7372 |      43718 |            |            |
  ;;    |  :resourced-provision-capacity |    ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     2040 |      49050 |      0.000 |       1250 |
  ;;    |   :resourced-provision-on-roll |      ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     2008 |      49082 |      0.000 |       1872 |
  ;;    |       :sen-provision-types-vec |                               |                              SEN Provision Types (derived) | :persistent-vector |    51090 |          0 |            |            |

  )

