(ns witan.gias
  "Read in \"Get Information About Schools\" (GIAS) all establishment data from CSV file downloaded from [get-information-schools.service.gov.uk/Downloads](https://www.get-information-schools.service.gov.uk/Downloads)"
  (:require [clojure.java.io :as io]
            [clojure.set     :as set]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]))



;;; # Data files
(def default-resource-file-name
  "Name of resource file containing default establishment data"
  "edubasealldata20230918.csv")



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))



;;; # Column names, labels and types
(def csv-col-name->order
  "Map CSV column name to order."
  (let [m (zipmap ["URN"
                   "LA (code)"
                   "LA (name)"
                   "EstablishmentNumber"
                   "EstablishmentName"
                   "TypeOfEstablishment (code)"
                   "TypeOfEstablishment (name)"
                   "EstablishmentTypeGroup (code)"
                   "EstablishmentTypeGroup (name)"
                   "EstablishmentStatus (code)"
                   "EstablishmentStatus (name)"
                   "ReasonEstablishmentOpened (code)"
                   "ReasonEstablishmentOpened (name)"
                   "OpenDate"
                   "ReasonEstablishmentClosed (code)"
                   "ReasonEstablishmentClosed (name)"
                   "CloseDate"
                   "PhaseOfEducation (code)"
                   "PhaseOfEducation (name)"
                   "StatutoryLowAge"
                   "StatutoryHighAge"
                   "Boarders (code)"
                   "Boarders (name)"
                   "NurseryProvision (name)"
                   "OfficialSixthForm (code)"
                   "OfficialSixthForm (name)"
                   "Gender (code)"
                   "Gender (name)"
                   "ReligiousCharacter (code)"
                   "ReligiousCharacter (name)"
                   "ReligiousEthos (name)"
                   "Diocese (code)"
                   "Diocese (name)"
                   "AdmissionsPolicy (code)"
                   "AdmissionsPolicy (name)"
                   "SchoolCapacity"
                   "SpecialClasses (code)"
                   "SpecialClasses (name)"
                   "CensusDate"
                   "NumberOfPupils"
                   "NumberOfBoys"
                   "NumberOfGirls"
                   "PercentageFSM"
                   "TrustSchoolFlag (code)"
                   "TrustSchoolFlag (name)"
                   "Trusts (code)"
                   "Trusts (name)"
                   "SchoolSponsorFlag (name)"
                   "SchoolSponsors (name)"
                   "FederationFlag (name)"
                   "Federations (code)"
                   "Federations (name)"
                   "UKPRN"
                   "FEHEIdentifier"
                   "FurtherEducationType (name)"
                   "OfstedLastInsp"
                   "OfstedSpecialMeasures (code)"
                   "OfstedSpecialMeasures (name)"
                   "LastChangedDate"
                   "Street"
                   "Locality"
                   "Address3"
                   "Town"
                   "County (name)"
                   "Postcode"
                   "SchoolWebsite"
                   "TelephoneNum"
                   "HeadTitle (name)"
                   "HeadFirstName"
                   "HeadLastName"
                   "HeadPreferredJobTitle"
                   "BSOInspectorateName (name)"
                   "InspectorateReport"
                   "DateOfLastInspectionVisit"
                   "NextInspectionVisit"
                   "TeenMoth (name)"
                   "TeenMothPlaces"
                   "CCF (name)"
                   "SENPRU (name)"
                   "EBD (name)"
                   "PlacesPRU"
                   "FTProv (name)"
                   "EdByOther (name)"
                   "Section41Approved (name)"
                   "SEN1 (name)"
                   "SEN2 (name)"
                   "SEN3 (name)"
                   "SEN4 (name)"
                   "SEN5 (name)"
                   "SEN6 (name)"
                   "SEN7 (name)"
                   "SEN8 (name)"
                   "SEN9 (name)"
                   "SEN10 (name)"
                   "SEN11 (name)"
                   "SEN12 (name)"
                   "SEN13 (name)"
                   "TypeOfResourcedProvision (name)"
                   "ResourcedProvisionOnRoll"
                   "ResourcedProvisionCapacity"
                   "SenUnitOnRoll"
                   "SenUnitCapacity"
                   "GOR (code)"
                   "GOR (name)"
                   "DistrictAdministrative (code)"
                   "DistrictAdministrative (name)"
                   "AdministrativeWard (code)"
                   "AdministrativeWard (name)"
                   "ParliamentaryConstituency (code)"
                   "ParliamentaryConstituency (name)"
                   "UrbanRural (code)"
                   "UrbanRural (name)"
                   "GSSLACode (name)"
                   "Easting"
                   "Northing"
                   "MSOA (name)"
                   "LSOA (name)"
                   "InspectorateName (name)"
                   "SENStat"
                   "SENNoStat"
                   "BoardingEstablishment (name)"
                   "PropsName"
                   "PreviousLA (code)"
                   "PreviousLA (name)"
                   "PreviousEstablishmentNumber"
                   "OfstedRating (name)"
                   "RSCRegion (name)"
                   "Country (name)"
                   "UPRN"
                   "SiteName"
                   "QABName (code)"
                   "QABName (name)"
                   "EstablishmentAccredited (code)"
                   "EstablishmentAccredited (name)"
                   "QABReport"
                   "CHNumber"
                   "MSOA (code)"
                   "LSOA (code)"
                   "FSM"
                   "AccreditationExpiryDate"]
                  (iterate inc 1))]
    (into (sorted-map-by (partial compare-mapped-keys m)) m)))

(def csv-col-names
  (apply sorted-set-by
         (partial compare-mapped-keys csv-col-name->order)
         (keys csv-col-name->order)))

(def csv-col-name->label
  "Map CSV column name to label.

  Labels adapted from [www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate](https://www.get-information-schools.service.gov.uk/Guidance/EstablishmentBulkUpdate)."
  (into (sorted-map-by (partial compare-mapped-keys csv-col-name->order)) 
        {"URN"                              "URN"
         "LA (code)"                        "LA (code)"
         "LA (name)"                        "LA"
         "EstablishmentNumber"              "Establishment Number"
         "EstablishmentName"                "School / College Name"
         "TypeOfEstablishment (code)"       "Establishment type (code)"
         "TypeOfEstablishment (name)"       "Establishment type"
         "EstablishmentTypeGroup (code)"    "Establishment type group (code)"
         "EstablishmentTypeGroup (name)"    "Establishment type group"
         "EstablishmentStatus (code)"       "Establishment status (code)"
         "EstablishmentStatus (name)"       "Establishment status"
         "ReasonEstablishmentOpened (code)" "Reason establishment opened (code)"
         "ReasonEstablishmentOpened (name)" "Reason establishment opened"
         "OpenDate"                         "Open date"
         "ReasonEstablishmentClosed (code)" "Reason establishment closed (code)"
         "ReasonEstablishmentClosed (name)" "Reason establishment closed"
         "CloseDate"                        "Close date"
         "PhaseOfEducation (code)"          "Phase of education (code)"
         "PhaseOfEducation (name)"          "Phase of education"
         "StatutoryLowAge"                  "Age range (low)"
         "StatutoryHighAge"                 "Age range (high)"
         "Boarders (code)"                  "Boarders (code)"
         "Boarders (name)"                  "Boarders"
         "NurseryProvision (name)"          "Nursery provision"
         "OfficialSixthForm (code)"         "Official sixth form (code)"
         "OfficialSixthForm (name)"         "Official sixth form"
         "Gender (code)"                    "Gender of entry (code)"
         "Gender (name)"                    "Gender of entry"
         "ReligiousCharacter (code)"        "Religious character (code)"
         "ReligiousCharacter (name)"        "Religious character"
         "ReligiousEthos (name)"            "Religious ethos"
         "Diocese (code)"                   "Diocese (code)"
         "Diocese (name)"                   "Diocese"
         "AdmissionsPolicy (code)"          "Admissons policy (code)"
         "AdmissionsPolicy (name)"          "Admissons policy"
         "SchoolCapacity"                   "School capacity"
         "SpecialClasses (code)"            "Special classes (code)"
         "SpecialClasses (name)"            "Special classes"
         "CensusDate"                       "Census date"
         "NumberOfPupils"                   "Number of pupils"
         "NumberOfBoys"                     "Number of boys"
         "NumberOfGirls"                    "Number of girls"
         "PercentageFSM"                    "Percentage FSM"
         "TrustSchoolFlag (code)"           "Trust school flag (code)"
         "TrustSchoolFlag (name)"           "Trust school flag"
         "Trusts (code)"                    "Academy trust or trust (code)"
         "Trusts (name)"                    "Academy trust or trust"
         "SchoolSponsorFlag (name)"         "School sponsor flag"
         "SchoolSponsors (name)"            "Academy sponsor"
         "FederationFlag (name)"            "Federation flag"
         "Federations (code)"               "Federation (code)"
         "Federations (name)"               "Federation"
         "UKPRN"                            "UK provider reference number (UKPRN)"
         "FEHEIdentifier"                   "FEHE identifier"
         "FurtherEducationType (name)"      "Further education type"
         "OfstedLastInsp"                   "Date of last OFSTED inspection"
         "OfstedSpecialMeasures (code)"     "OFSTED special measures (code)"
         "OfstedSpecialMeasures (name)"     "OFSTED special measures"
         "LastChangedDate"                  "Last Changed Date"
         "Street"                           "Street"
         "Locality"                         "Locality"
         "Address3"                         "Address 3"
         "Town"                             "Town"
         "County (name)"                    "County"
         "Postcode"                         "Postcode"
         "SchoolWebsite"                    "Website"
         "TelephoneNum"                     "Telephone"
         "HeadTitle (name)"                 "Headteacher/Principal title"
         "HeadFirstName"                    "Headteacher/Principal first name"
         "HeadLastName"                     "Headteacher/Principal last name"
         "HeadPreferredJobTitle"            "Headteacher/Principal preferred job title"
         "BSOInspectorateName (name)"       "BSO inspectorate name"
         "InspectorateReport"               "Inspectorate report URL"
         "DateOfLastInspectionVisit"        "Date of last inspection visit"
         "NextInspectionVisit"              "Date of next inspection visit"
         "TeenMoth (name)"                  "Teenage mothers"
         "TeenMothPlaces"                   "Teenage mothers capacity"
         "CCF (name)"                       "Child care facilities"
         "SENPRU (name)"                    "PRU provision for SEN"
         "EBD (name)"                       "PRU provision for EBD"
         "PlacesPRU"                        "Number of PRU places"
         "FTProv (name)"                    "PRU offer full time provision"
         "EdByOther (name)"                 "PRU offer tuition by anther provider"
         "Section41Approved (name)"         "Section 41 approved"
         "SEN1 (name)"                      "SEN need 1"
         "SEN2 (name)"                      "SEN need 2"
         "SEN3 (name)"                      "SEN need 3"
         "SEN4 (name)"                      "SEN need 4"
         "SEN5 (name)"                      "SEN need 5"
         "SEN6 (name)"                      "SEN need 6"
         "SEN7 (name)"                      "SEN need 7"
         "SEN8 (name)"                      "SEN need 8"
         "SEN9 (name)"                      "SEN need 9"
         "SEN10 (name)"                     "SEN need 10"
         "SEN11 (name)"                     "SEN need 11"
         "SEN12 (name)"                     "SEN need 12"
         "SEN13 (name)"                     "SEN need 13"
         "TypeOfResourcedProvision (name)"  "Type of resourced provision"
         "ResourcedProvisionOnRoll"         "Resourced provision number on roll"
         "ResourcedProvisionCapacity"       "Resourced provision capacity"
         "SenUnitOnRoll"                    "SEN unit number on roll"
         "SenUnitCapacity"                  "SEN unit capacity"
         "GOR (code)"                       "GOR (code)"
         "GOR (name)"                       "GOR"
         "DistrictAdministrative (code)"    "District administrative (code)"
         "DistrictAdministrative (name)"    "District administrative"
         "AdministrativeWard (code)"        "Administrative ward (code)"
         "AdministrativeWard (name)"        "Administrative ward"
         "ParliamentaryConstituency (code)" "Parliamentary constituency (code)"
         "ParliamentaryConstituency (name)" "Parliamentary constituency"
         "UrbanRural (code)"                "Urban rural (code)"
         "UrbanRural (name)"                "Urban rural"
         "GSSLACode (name)"                 "GSSLA code"
         "Easting"                          "Easting"
         "Northing"                         "Northing"
         "MSOA (name)"                      "MSOA"
         "LSOA (name)"                      "LSOA"
         "InspectorateName (name)"          "Inspectorate name"
         "SENStat"                          "Number of special pupils under a SEN statement or EHCP"
         "SENNoStat"                        "Number of special pupils not under a SEN statement or EHCP"
         "BoardingEstablishment (name)"     "Boarding establishment"
         "PropsName"                        "Proprietor's name"
         "PreviousLA (code)"                "Previous local authority (code)"
         "PreviousLA (name)"                "Previous local authority"
         "PreviousEstablishmentNumber"      "Previous establishment number"
         "OfstedRating (name)"              "OFSTED rating"
         "RSCRegion (name)"                 "RSC region"
         "Country (name)"                   "Country"
         "UPRN"                             "UPRN"
         "SiteName"                         "Site name"
         "QABName (code)"                   "QAB name (code)"
         "QABName (name)"                   "QAB name"
         "EstablishmentAccredited (code)"   "Establishment accredited (code)"
         "EstablishmentAccredited (name)"   "Establishment accredited"
         "QABReport"                        "QAB report"
         "CHNumber"                         "CH number"
         "MSOA (code)"                      "MSOA (code)"
         "LSOA (code)"                      "LSOA (code)"
         "FSM"                              "FSM"
         "AccreditationExpiryDate"          "Accreditation expiry date"}))

(def csv-col-name->col-name
  "Map CSV column name to name to be used in dataset."
  (into (sorted-map-by (partial compare-mapped-keys csv-col-name->order)) 
        {"URN"                              :urn
         "LA (code)"                        :la-code
         "LA (name)"                        :la-name
         "EstablishmentNumber"              :establishment-number
         "EstablishmentName"                :establishment-name
         "TypeOfEstablishment (code)"       :type-of-establishment-code
         "TypeOfEstablishment (name)"       :type-of-establishment-name
         "EstablishmentTypeGroup (code)"    :establishment-type-group-code
         "EstablishmentTypeGroup (name)"    :establishment-type-group-name
         "EstablishmentStatus (code)"       :establishment-status-code
         "EstablishmentStatus (name)"       :establishment-status-name
         "ReasonEstablishmentOpened (code)" :reason-establishment-opened-code
         "ReasonEstablishmentOpened (name)" :reason-establishment-opened-name
         "OpenDate"                         :open-date
         "ReasonEstablishmentClosed (code)" :reason-establishment-closed-code
         "ReasonEstablishmentClosed (name)" :reason-establishment-closed-name
         "CloseDate"                        :close-date
         "PhaseOfEducation (code)"          :phase-of-education-code
         "PhaseOfEducation (name)"          :phase-of-education-name
         "StatutoryLowAge"                  :statutory-low-age
         "StatutoryHighAge"                 :statutory-high-age
         "Boarders (code)"                  :boarders-code
         "Boarders (name)"                  :boarders-name
         "NurseryProvision (name)"          :nursery-provision-name
         "OfficialSixthForm (code)"         :official-sixth-form-code
         "OfficialSixthForm (name)"         :official-sixth-form-name
         "Gender (code)"                    :gender-code
         "Gender (name)"                    :gender-name
         "ReligiousCharacter (code)"        :religious-character-code
         "ReligiousCharacter (name)"        :religious-character-name
         "ReligiousEthos (name)"            :religious-ethos-name
         "Diocese (code)"                   :diocese-code
         "Diocese (name)"                   :diocese-name
         "AdmissionsPolicy (code)"          :admissions-policy-code
         "AdmissionsPolicy (name)"          :admissions-policy-name
         "SchoolCapacity"                   :school-capacity
         "SpecialClasses (code)"            :special-classes-code
         "SpecialClasses (name)"            :special-classes-name
         "CensusDate"                       :census-date
         "NumberOfPupils"                   :number-of-pupils
         "NumberOfBoys"                     :number-of-boys
         "NumberOfGirls"                    :number-of-girls
         "PercentageFSM"                    :percentage-fsm
         "TrustSchoolFlag (code)"           :trust-school-flag-code
         "TrustSchoolFlag (name)"           :trust-school-flag-name
         "Trusts (code)"                    :trusts-code
         "Trusts (name)"                    :trusts-name
         "SchoolSponsorFlag (name)"         :school-sponsor-flag-name
         "SchoolSponsors (name)"            :school-sponsors-name
         "FederationFlag (name)"            :federation-flag-name
         "Federations (code)"               :federations-code
         "Federations (name)"               :federations-name
         "UKPRN"                            :ukprn
         "FEHEIdentifier"                   :fehe-identifier
         "FurtherEducationType (name)"      :further-education-type-name
         "OfstedLastInsp"                   :ofsted-last-insp
         "OfstedSpecialMeasures (code)"     :ofsted-special-measures-code
         "OfstedSpecialMeasures (name)"     :ofsted-special-measures-name
         "LastChangedDate"                  :last-changed-date
         "Street"                           :street
         "Locality"                         :locality
         "Address3"                         :address3
         "Town"                             :town
         "County (name)"                    :county-name
         "Postcode"                         :postcode
         "SchoolWebsite"                    :school-website
         "TelephoneNum"                     :telephone-num
         "HeadTitle (name)"                 :head-title-name
         "HeadFirstName"                    :head-first-name
         "HeadLastName"                     :head-last-name
         "HeadPreferredJobTitle"            :head-preferred-job-title
         "BSOInspectorateName (name)"       :bso-inspectorate-name-name
         "InspectorateReport"               :inspectorate-report
         "DateOfLastInspectionVisit"        :date-of-last-inspection-visit
         "NextInspectionVisit"              :next-inspection-visit
         "TeenMoth (name)"                  :teen-moth-name
         "TeenMothPlaces"                   :teen-moth-places
         "CCF (name)"                       :ccf-name
         "SENPRU (name)"                    :senpru-name
         "EBD (name)"                       :ebd-name
         "PlacesPRU"                        :places-pru
         "FTProv (name)"                    :ft-prov-name
         "EdByOther (name)"                 :ed-by-other-name
         "Section41Approved (name)"         :section41-approved-name
         "SEN1 (name)"                      :sen1-name
         "SEN2 (name)"                      :sen2-name
         "SEN3 (name)"                      :sen3-name
         "SEN4 (name)"                      :sen4-name
         "SEN5 (name)"                      :sen5-name
         "SEN6 (name)"                      :sen6-name
         "SEN7 (name)"                      :sen7-name
         "SEN8 (name)"                      :sen8-name
         "SEN9 (name)"                      :sen9-name
         "SEN10 (name)"                     :sen10-name
         "SEN11 (name)"                     :sen11-name
         "SEN12 (name)"                     :sen12-name
         "SEN13 (name)"                     :sen13-name
         "TypeOfResourcedProvision (name)"  :type-of-resourced-provision-name
         "ResourcedProvisionOnRoll"         :resourced-provision-on-roll
         "ResourcedProvisionCapacity"       :resourced-provision-capacity
         "SenUnitOnRoll"                    :sen-unit-on-roll
         "SenUnitCapacity"                  :sen-unit-capacity
         "GOR (code)"                       :gor-code
         "GOR (name)"                       :gor-name
         "DistrictAdministrative (code)"    :district-administrative-code
         "DistrictAdministrative (name)"    :district-administrative-name
         "AdministrativeWard (code)"        :administrative-ward-code
         "AdministrativeWard (name)"        :administrative-ward-name
         "ParliamentaryConstituency (code)" :parliamentary-constituency-code
         "ParliamentaryConstituency (name)" :parliamentary-constituency-name
         "UrbanRural (code)"                :urban-rural-code
         "UrbanRural (name)"                :urban-rural-name
         "GSSLACode (name)"                 :gssla-code-name
         "Easting"                          :easting
         "Northing"                         :northing
         "MSOA (name)"                      :msoa-name
         "LSOA (name)"                      :lsoa-name
         "InspectorateName (name)"          :inspectorate-name-name
         "SENStat"                          :sen-stat
         "SENNoStat"                        :sen-no-stat
         "BoardingEstablishment (name)"     :boarding-establishment-name
         "PropsName"                        :props-name
         "PreviousLA (code)"                :previous-la-code
         "PreviousLA (name)"                :previous-la-name
         "PreviousEstablishmentNumber"      :previous-establishment-number
         "OfstedRating (name)"              :ofsted-rating-name
         "RSCRegion (name)"                 :rsc-region-name
         "Country (name)"                   :country-name
         "UPRN"                             :uprn
         "SiteName"                         :site-name
         "QABName (code)"                   :qab-name-code
         "QABName (name)"                   :qab-name-name
         "EstablishmentAccredited (code)"   :establishment-accredited-code
         "EstablishmentAccredited (name)"   :establishment-accredited-name
         "QABReport"                        :qab-report
         "CHNumber"                         :ch-number
         "MSOA (code)"                      :msoa-code
         "LSOA (code)"                      :lsoa-code
         "FSM"                              :fsm
         "AccreditationExpiryDate"          :accreditation-expiry-date}))

(comment
  ;; Keyword column names created from CSV column lables using:
  (let [csv-col-name->keyword-col-name (fn [s] (-> s
                                                   (clojure.string/replace #"(?<!^)([A-Z])(?=[a-z])" "-$1")
                                                   (clojure.string/replace #"(?<=[a-z])([A-Z])" "-$1")
                                                   (clojure.string/replace #"[()]" "")
                                                   (clojure.string/replace #" +" "-")
                                                   (clojure.string/lower-case)
                                                   keyword))]
    (into (sorted-map-by (partial compare-mapped-keys csv-col-name->order)) 
          (map (fn [s] [s (csv-col-name->keyword-col-name s)]))
          csv-col-names))

  )

(def col-name->order
  (let [m (update-keys csv-col-name->order csv-col-name->col-name)]
    (into (sorted-map-by (partial compare-mapped-keys m)) m)))

(def col-names
  (apply sorted-set-by
         (partial compare-mapped-keys col-name->order)
         (keys col-name->order)))

(def col-name->csv-col-name
  (into (sorted-map-by (partial compare-mapped-keys col-name->order)) 
        (set/map-invert csv-col-name->col-name)))

(def col-name->label
  "Map column name to label."
  (into (sorted-map-by (partial compare-mapped-keys col-name->order))
        (update-keys csv-col-name->label csv-col-name->col-name)))



;;; # Read establishment data
(def csv-parser-fn
  "parser-fn for ds/->dataset read of edubasealldata.csv with CSV column names."
  (let [parse-date (fn [s] (java.time.LocalDate/parse s (java.time.format.DateTimeFormatter/ofPattern "dd-MM-uuuu")))]
    (into (sorted-map-by (partial compare-mapped-keys csv-col-name->order))
          {"URN"                         #_:urn                           :string
           "LA (code)"                   #_:la-code                       :string
           "EstablishmentNumber"         #_:establishment-number          :string
           "OpenDate"                    #_:open-date                     [:packed-local-date parse-date]
           "CloseDate"                   #_:close-date                    [:packed-local-date parse-date]
           "CensusDate"                  #_:census-date                   [:packed-local-date parse-date]
           "UKPRN"                       #_:ukprn                         :string
           "FEHEIdentifier"              #_:fehe-identifier               :string
           "OfstedLastInsp"              #_:ofsted-last-insp              [:packed-local-date parse-date]
           "LastChangedDate"             #_:last-changed-date             [:packed-local-date parse-date]
           "TelephoneNum"                #_:telephone-num                 :string
           "DateOfLastInspectionVisit"   #_:date-of-last-inspection-visit [:packed-local-date parse-date]
           "NextInspectionVisit"         #_:next-inspection-visit         [:packed-local-date parse-date]
           "PreviousLA (code)"           #_:previous-la-code              :string
           "PreviousEstablishmentNumber" #_:previous-establishment-number :string
           "UPRN"                        #_:uprn                          :string
           "QABReport"                   #_:qab-report                    :string
           "CHNumber"                    #_:ch-number                     :string})))

(def key-fn
  "Default key-fn to be applied to edubasealldata.csv column names to obtain dataset column names."
  #(csv-col-name->col-name % %))

(def parser-fn
  "parser-fn for ds/->dataset read of edubasealldata.csv with `key-fn` applied to CSV column names."
  (into (sorted-map-by (partial compare-mapped-keys col-name->order))
        (update-keys csv-parser-fn key-fn)))

(defn ->ds
  "Read GIAS all establishment data into a dataset.
   Use optional `options` map to specify:
   - CSV file to read: via ::file-path or ::resource-file-name (for files in resource folder).
     [Defaults to ::resource-file-name `default-resource-file-name`.]
   - Additional or over-riding options for `->dataset`."
  ([] (->ds {}))
  ([{::keys [resource-file-name file-path]
     :or    {resource-file-name default-resource-file-name}
     :as    options}]
   (with-open [in (-> (or file-path (io/resource resource-file-name))
                      io/file
                      io/input-stream)]
     (ds/->dataset in (merge {:file-type    :csv
                              :separator    ","
                              :dataset-name (or file-path resource-file-name)
                              :header-row?  true
                              :key-fn       key-fn
                              :parser-fn    parser-fn}
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

  (-> (->ds
       #_{::file-path "/tmp/edubasealldata20230421.csv"}
       #_{::resource-file-name "edubasealldata20230421.csv"}
       #_{::resource-file-name "edubasealldata20230817.csv"}
       #_{::resource-file-name "edubasealldata20230918.csv"}
       )
      (csv-ds-column-info col-name->csv-col-name col-name->label)
      (vary-meta assoc :print-index-range 1000))
  ;; => edubasealldata20230918.csv: descriptive-stats [140 8]:
  ;;    |                         :col-name |                    :csv-col-name |                                                 :col-label |          :datatype | :n-valid | :n-missing |       :min |       :max |
  ;;    |-----------------------------------|----------------------------------|------------------------------------------------------------|--------------------|---------:|-----------:|------------|------------|
  ;;    |                              :urn |                              URN |                                                        URN |            :string |    50421 |          0 |            |            |
  ;;    |                          :la-code |                        LA (code) |                                                  LA (code) |            :string |    50421 |          0 |            |            |
  ;;    |                          :la-name |                        LA (name) |                                                         LA |            :string |    50421 |          0 |            |            |
  ;;    |             :establishment-number |              EstablishmentNumber |                                       Establishment Number |            :string |    50286 |        135 |            |            |
  ;;    |               :establishment-name |                EstablishmentName |                                      School / College Name |            :string |    50421 |          0 |            |            |
  ;;    |       :type-of-establishment-code |       TypeOfEstablishment (code) |                                  Establishment type (code) |             :int16 |    50421 |          0 |      1.000 |      56.00 |
  ;;    |       :type-of-establishment-name |       TypeOfEstablishment (name) |                                         Establishment type |            :string |    50421 |          0 |            |            |
  ;;    |    :establishment-type-group-code |    EstablishmentTypeGroup (code) |                            Establishment type group (code) |             :int16 |    50421 |          0 |      1.000 |      11.00 |
  ;;    |    :establishment-type-group-name |    EstablishmentTypeGroup (name) |                                   Establishment type group |            :string |    50421 |          0 |            |            |
  ;;    |        :establishment-status-code |       EstablishmentStatus (code) |                                Establishment status (code) |             :int16 |    50421 |          0 |      1.000 |      4.000 |
  ;;    |        :establishment-status-name |       EstablishmentStatus (name) |                                       Establishment status |            :string |    50421 |          0 |            |            |
  ;;    | :reason-establishment-opened-code | ReasonEstablishmentOpened (code) |                         Reason establishment opened (code) |             :int16 |    50421 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-opened-name | ReasonEstablishmentOpened (name) |                                Reason establishment opened |            :string |    49737 |        684 |            |            |
  ;;    |                        :open-date |                         OpenDate |                                                  Open date | :packed-local-date |    19920 |      30501 | 1800-01-01 | 2024-01-01 |
  ;;    | :reason-establishment-closed-code | ReasonEstablishmentClosed (code) |                         Reason establishment closed (code) |             :int16 |    50421 |          0 |      0.000 |      99.00 |
  ;;    | :reason-establishment-closed-name | ReasonEstablishmentClosed (name) |                                Reason establishment closed |            :string |    38076 |      12345 |            |            |
  ;;    |                       :close-date |                        CloseDate |                                                 Close date | :packed-local-date |    22990 |      27431 | 1900-01-01 | 2026-08-31 |
  ;;    |          :phase-of-education-code |          PhaseOfEducation (code) |                                  Phase of education (code) |             :int16 |    50421 |          0 |      0.000 |      7.000 |
  ;;    |          :phase-of-education-name |          PhaseOfEducation (name) |                                         Phase of education |            :string |    50421 |          0 |            |            |
  ;;    |                :statutory-low-age |                  StatutoryLowAge |                                            Age range (low) |             :int16 |    46461 |       3960 |      0.000 |      19.00 |
  ;;    |               :statutory-high-age |                 StatutoryHighAge |                                           Age range (high) |             :int16 |    46464 |       3957 |      3.000 |      99.00 |
  ;;    |                    :boarders-code |                  Boarders (code) |                                            Boarders (code) |             :int16 |    50421 |          0 |      0.000 |      9.000 |
  ;;    |                    :boarders-name |                  Boarders (name) |                                                   Boarders |            :string |    48800 |       1621 |            |            |
  ;;    |           :nursery-provision-name |          NurseryProvision (name) |                                          Nursery provision |            :string |    50393 |         28 |            |            |
  ;;    |         :official-sixth-form-code |         OfficialSixthForm (code) |                                 Official sixth form (code) |             :int16 |    50421 |          0 |      0.000 |      9.000 |
  ;;    |         :official-sixth-form-name |         OfficialSixthForm (name) |                                        Official sixth form |            :string |    50414 |          7 |            |            |
  ;;    |                      :gender-code |                    Gender (code) |                                     Gender of entry (code) |             :int16 |    50421 |          0 |      0.000 |      9.000 |
  ;;    |                      :gender-name |                    Gender (name) |                                            Gender of entry |            :string |    49011 |       1410 |            |            |
  ;;    |         :religious-character-code |        ReligiousCharacter (code) |                                 Religious character (code) |             :int16 |    50421 |          0 |      0.000 |      99.00 |
  ;;    |         :religious-character-name |        ReligiousCharacter (name) |                                        Religious character |            :string |    46730 |       3691 |            |            |
  ;;    |             :religious-ethos-name |            ReligiousEthos (name) |                                            Religious ethos |            :string |    47156 |       3265 |            |            |
  ;;    |                     :diocese-code |                   Diocese (code) |                                             Diocese (code) |            :string |    50421 |          0 |            |            |
  ;;    |                     :diocese-name |                   Diocese (name) |                                                    Diocese |            :string |    49108 |       1313 |            |            |
  ;;    |           :admissions-policy-code |          AdmissionsPolicy (code) |                                    Admissons policy (code) |             :int16 |    50421 |          0 |      0.000 |      9.000 |
  ;;    |           :admissions-policy-name |          AdmissionsPolicy (name) |                                           Admissons policy |            :string |    45205 |       5216 |            |            |
  ;;    |                  :school-capacity |                   SchoolCapacity |                                            School capacity |             :int16 |    38107 |      12314 |      1.000 |  1.000E+04 |
  ;;    |             :special-classes-code |            SpecialClasses (code) |                                     Special classes (code) |             :int16 |    50421 |          0 |      0.000 |      9.000 |
  ;;    |             :special-classes-name |            SpecialClasses (name) |                                            Special classes |            :string |    50301 |        120 |            |            |
  ;;    |                      :census-date |                       CensusDate |                                                Census date | :packed-local-date |    29058 |      21363 | 2017-01-19 | 2023-01-19 |
  ;;    |                 :number-of-pupils |                   NumberOfPupils |                                           Number of pupils |             :int16 |    29052 |      21369 |      0.000 |       3440 |
  ;;    |                   :number-of-boys |                     NumberOfBoys |                                             Number of boys |             :int16 |    29045 |      21376 |      0.000 |       1766 |
  ;;    |                  :number-of-girls |                    NumberOfGirls |                                            Number of girls |             :int16 |    29040 |      21381 |      0.000 |       1885 |
  ;;    |                   :percentage-fsm |                    PercentageFSM |                                             Percentage FSM |           :float64 |    28850 |      21571 |      0.000 |      100.0 |
  ;;    |           :trust-school-flag-code |           TrustSchoolFlag (code) |                                   Trust school flag (code) |             :int16 |    50421 |          0 |      0.000 |      5.000 |
  ;;    |           :trust-school-flag-name |           TrustSchoolFlag (name) |                                          Trust school flag |            :string |    50421 |          0 |            |            |
  ;;    |                      :trusts-code |                    Trusts (code) |                              Academy trust or trust (code) |             :int16 |    11012 |      39409 |       1028 |  1.770E+04 |
  ;;    |                      :trusts-name |                    Trusts (name) |                                     Academy trust or trust |            :string |    11012 |      39409 |            |            |
  ;;    |         :school-sponsor-flag-name |         SchoolSponsorFlag (name) |                                        School sponsor flag |            :string |    50421 |          0 |            |            |
  ;;    |             :school-sponsors-name |            SchoolSponsors (name) |                                            Academy sponsor |            :string |     7985 |      42436 |            |            |
  ;;    |             :federation-flag-name |            FederationFlag (name) |                                            Federation flag |            :string |    50421 |          0 |            |            |
  ;;    |                 :federations-code |               Federations (code) |                                          Federation (code) |            :string |     1125 |      49296 |            |            |
  ;;    |                 :federations-name |               Federations (name) |                                                 Federation |            :string |     1125 |      49296 |            |            |
  ;;    |                            :ukprn |                            UKPRN |                       UK provider reference number (UKPRN) |            :string |    31909 |      18512 |            |            |
  ;;    |                  :fehe-identifier |                   FEHEIdentifier |                                            FEHE identifier |            :string |      537 |      49884 |            |            |
  ;;    |      :further-education-type-name |      FurtherEducationType (name) |                                     Further education type |            :string |    46315 |       4106 |            |            |
  ;;    |                 :ofsted-last-insp |                   OfstedLastInsp |                             Date of last OFSTED inspection | :packed-local-date |    27103 |      23318 | 2006-05-12 | 2023-07-12 |
  ;;    |     :ofsted-special-measures-code |     OfstedSpecialMeasures (code) |                             OFSTED special measures (code) |             :int16 |    50421 |          0 |      0.000 |      0.000 |
  ;;    |     :ofsted-special-measures-name |     OfstedSpecialMeasures (name) |                                    OFSTED special measures |            :string |    50421 |          0 |            |            |
  ;;    |                :last-changed-date |                  LastChangedDate |                                          Last Changed Date | :packed-local-date |    50421 |          0 | 2013-10-24 | 2023-09-18 |
  ;;    |                           :street |                           Street |                                                     Street |            :string |    49228 |       1193 |            |            |
  ;;    |                         :locality |                         Locality |                                                   Locality |            :string |    27341 |      23080 |            |            |
  ;;    |                         :address3 |                         Address3 |                                                  Address 3 |            :string |     3993 |      46428 |            |            |
  ;;    |                             :town |                             Town |                                                       Town |            :string |    48423 |       1998 |            |            |
  ;;    |                      :county-name |                    County (name) |                                                     County |            :string |    38804 |      11617 |            |            |
  ;;    |                         :postcode |                         Postcode |                                                   Postcode |            :string |    49005 |       1416 |            |            |
  ;;    |                   :school-website |                    SchoolWebsite |                                                    Website |            :string |    24600 |      25821 |            |            |
  ;;    |                    :telephone-num |                     TelephoneNum |                                                  Telephone |            :string |    27343 |      23078 |            |            |
  ;;    |                  :head-title-name |                 HeadTitle (name) |                                Headteacher/Principal title |            :string |    41410 |       9011 |            |            |
  ;;    |                  :head-first-name |                    HeadFirstName |                           Headteacher/Principal first name |            :string |    42770 |       7651 |            |            |
  ;;    |                   :head-last-name |                     HeadLastName |                            Headteacher/Principal last name |            :string |    42828 |       7593 |            |            |
  ;;    |         :head-preferred-job-title |            HeadPreferredJobTitle |                  Headteacher/Principal preferred job title |            :string |    44592 |       5829 |            |            |
  ;;    |       :bso-inspectorate-name-name |       BSOInspectorateName (name) |                                      BSO inspectorate name |            :string |    50421 |          0 |            |            |
  ;;    |              :inspectorate-report |               InspectorateReport |                                    Inspectorate report URL |            :string |      248 |      50173 |            |            |
  ;;    |    :date-of-last-inspection-visit |        DateOfLastInspectionVisit |                              Date of last inspection visit | :packed-local-date |      249 |      50172 | 2016-03-03 | 2023-06-15 |
  ;;    |            :next-inspection-visit |              NextInspectionVisit |                              Date of next inspection visit | :packed-local-date |        0 |      50421 | 1970-01-01 | 1970-01-01 |
  ;;    |                   :teen-moth-name |                  TeenMoth (name) |                                            Teenage mothers |            :string |    50412 |          9 |            |            |
  ;;    |                 :teen-moth-places |                   TeenMothPlaces |                                   Teenage mothers capacity |             :int16 |       98 |      50323 |      0.000 |      45.00 |
  ;;    |                         :ccf-name |                       CCF (name) |                                      Child care facilities |            :string |    44688 |       5733 |            |            |
  ;;    |                      :senpru-name |                    SENPRU (name) |                                      PRU provision for SEN |            :string |    50410 |         11 |            |            |
  ;;    |                         :ebd-name |                       EBD (name) |                                      PRU provision for EBD |            :string |    50414 |          7 |            |            |
  ;;    |                       :places-pru |                        PlacesPRU |                                       Number of PRU places |             :int16 |      602 |      49819 |      0.000 |      300.0 |
  ;;    |                     :ft-prov-name |                    FTProv (name) |                              PRU offer full time provision |            :string |     1789 |      48632 |            |            |
  ;;    |                 :ed-by-other-name |                 EdByOther (name) |                       PRU offer tuition by anther provider |            :string |    44677 |       5744 |            |            |
  ;;    |          :section41-approved-name |         Section41Approved (name) |                                        Section 41 approved |            :string |    50421 |          0 |            |            |
  ;;    |                        :sen1-name |                      SEN1 (name) |                                                 SEN need 1 |            :string |     5599 |      44822 |            |            |
  ;;    |                        :sen2-name |                      SEN2 (name) |                                                 SEN need 2 |            :string |     1974 |      48447 |            |            |
  ;;    |                        :sen3-name |                      SEN3 (name) |                                                 SEN need 3 |            :string |     1177 |      49244 |            |            |
  ;;    |                        :sen4-name |                      SEN4 (name) |                                                 SEN need 4 |            :string |      819 |      49602 |            |            |
  ;;    |                        :sen5-name |                      SEN5 (name) |                                                 SEN need 5 |            :string |      594 |      49827 |            |            |
  ;;    |                        :sen6-name |                      SEN6 (name) |                                                 SEN need 6 |            :string |      516 |      49905 |            |            |
  ;;    |                        :sen7-name |                      SEN7 (name) |                                                 SEN need 7 |            :string |      460 |      49961 |            |            |
  ;;    |                        :sen8-name |                      SEN8 (name) |                                                 SEN need 8 |            :string |      387 |      50034 |            |            |
  ;;    |                        :sen9-name |                      SEN9 (name) |                                                 SEN need 9 |            :string |      304 |      50117 |            |            |
  ;;    |                       :sen10-name |                     SEN10 (name) |                                                SEN need 10 |            :string |      207 |      50214 |            |            |
  ;;    |                       :sen11-name |                     SEN11 (name) |                                                SEN need 11 |            :string |      142 |      50279 |            |            |
  ;;    |                       :sen12-name |                     SEN12 (name) |                                                SEN need 12 |            :string |       98 |      50323 |            |            |
  ;;    |                       :sen13-name |                     SEN13 (name) |                                                SEN need 13 |            :string |        5 |      50416 |            |            |
  ;;    | :type-of-resourced-provision-name |  TypeOfResourcedProvision (name) |                                Type of resourced provision |            :string |     7033 |      43388 |            |            |
  ;;    |      :resourced-provision-on-roll |         ResourcedProvisionOnRoll |                         Resourced provision number on roll |             :int16 |     1900 |      48521 |      0.000 |       1872 |
  ;;    |     :resourced-provision-capacity |       ResourcedProvisionCapacity |                               Resourced provision capacity |             :int16 |     1933 |      48488 |      0.000 |       1250 |
  ;;    |                 :sen-unit-on-roll |                    SenUnitOnRoll |                                    SEN unit number on roll |             :int16 |      867 |      49554 |      0.000 |      427.0 |
  ;;    |                :sen-unit-capacity |                  SenUnitCapacity |                                          SEN unit capacity |             :int16 |      886 |      49535 |      0.000 |      427.0 |
  ;;    |                         :gor-code |                       GOR (code) |                                                 GOR (code) |            :string |    50421 |          0 |            |            |
  ;;    |                         :gor-name |                       GOR (name) |                                                        GOR |            :string |    50421 |          0 |            |            |
  ;;    |     :district-administrative-code |    DistrictAdministrative (code) |                             District administrative (code) |            :string |    50421 |          0 |            |            |
  ;;    |     :district-administrative-name |    DistrictAdministrative (name) |                                    District administrative |            :string |    48999 |       1422 |            |            |
  ;;    |         :administrative-ward-code |        AdministrativeWard (code) |                                 Administrative ward (code) |            :string |    50421 |          0 |            |            |
  ;;    |         :administrative-ward-name |        AdministrativeWard (name) |                                        Administrative ward |            :string |    49000 |       1421 |            |            |
  ;;    |  :parliamentary-constituency-code | ParliamentaryConstituency (code) |                          Parliamentary constituency (code) |            :string |    50421 |          0 |            |            |
  ;;    |  :parliamentary-constituency-name | ParliamentaryConstituency (name) |                                 Parliamentary constituency |            :string |    49000 |       1421 |            |            |
  ;;    |                 :urban-rural-code |                UrbanRural (code) |                                         Urban rural (code) |            :string |    50421 |          0 |            |            |
  ;;    |                 :urban-rural-name |                UrbanRural (name) |                                                Urban rural |            :string |    48999 |       1422 |            |            |
  ;;    |                  :gssla-code-name |                 GSSLACode (name) |                                                 GSSLA code |            :string |    50421 |          0 |            |            |
  ;;    |                          :easting |                          Easting |                                                    Easting |             :int32 |    48700 |       1721 |      0.000 |  6.551E+05 |
  ;;    |                         :northing |                         Northing |                                                   Northing |             :int32 |    48700 |       1721 |      0.000 |  8.119E+05 |
  ;;    |                        :msoa-name |                      MSOA (name) |                                                       MSOA |            :string |    49000 |       1421 |            |            |
  ;;    |                        :lsoa-name |                      LSOA (name) |                                                       LSOA |            :string |    48999 |       1422 |            |            |
  ;;    |           :inspectorate-name-name |          InspectorateName (name) |                                          Inspectorate name |            :string |     4676 |      45745 |            |            |
  ;;    |                         :sen-stat |                          SENStat |     Number of special pupils under a SEN statement or EHCP |             :int16 |     3655 |      46766 |      0.000 |      311.0 |
  ;;    |                      :sen-no-stat |                        SENNoStat | Number of special pupils not under a SEN statement or EHCP |             :int16 |     3508 |      46913 |      0.000 |      485.0 |
  ;;    |      :boarding-establishment-name |     BoardingEstablishment (name) |                                     Boarding establishment |            :string |     2609 |      47812 |            |            |
  ;;    |                       :props-name |                        PropsName |                                          Proprietor's name |            :string |     3224 |      47197 |            |            |
  ;;    |                 :previous-la-code |                PreviousLA (code) |                            Previous local authority (code) |            :string |    50421 |          0 |            |            |
  ;;    |                 :previous-la-name |                PreviousLA (name) |                                   Previous local authority |            :string |    16589 |      33832 |            |            |
  ;;    |    :previous-establishment-number |      PreviousEstablishmentNumber |                              Previous establishment number |            :string |     8048 |      42373 |            |            |
  ;;    |               :ofsted-rating-name |              OfstedRating (name) |                                              OFSTED rating |            :string |    27044 |      23377 |            |            |
  ;;    |                  :rsc-region-name |                 RSCRegion (name) |                                                 RSC region |            :string |    47469 |       2952 |            |            |
  ;;    |                     :country-name |                   Country (name) |                                                    Country |            :string |     2777 |      47644 |            |            |
  ;;    |                             :uprn |                             UPRN |                                                       UPRN |            :string |    37729 |      12692 |            |            |
  ;;    |                        :site-name |                         SiteName |                                                  Site name |            :string |        3 |      50418 |            |            |
  ;;    |                    :qab-name-code |                   QABName (code) |                                            QAB name (code) |             :int16 |    50421 |          0 |      0.000 |      0.000 |
  ;;    |                    :qab-name-name |                   QABName (name) |                                                   QAB name |            :string |    50421 |          0 |            |            |
  ;;    |    :establishment-accredited-code |   EstablishmentAccredited (code) |                            Establishment accredited (code) |             :int16 |    50421 |          0 |      0.000 |      0.000 |
  ;;    |    :establishment-accredited-name |   EstablishmentAccredited (name) |                                   Establishment accredited |            :string |    50421 |          0 |            |            |
  ;;    |                       :qab-report |                        QABReport |                                                 QAB report |            :string |        0 |      50421 |            |            |
  ;;    |                        :ch-number |                         CHNumber |                                                  CH number |            :string |        0 |      50421 |            |            |
  ;;    |                        :msoa-code |                      MSOA (code) |                                                MSOA (code) |            :string |    50421 |          0 |            |            |
  ;;    |                        :lsoa-code |                      LSOA (code) |                                                LSOA (code) |            :string |    50421 |          0 |            |            |
  ;;    |                              :fsm |                              FSM |                                                        FSM |             :int16 |    28851 |      21570 |      0.000 |      925.0 |
  ;;    |        :accreditation-expiry-date |          AccreditationExpiryDate |                                  Accreditation expiry date |           :boolean |        0 |      50421 |            |            |

  )


(comment
  ;; Write distinct establishment types to data file
  (-> (->ds {:column-allowlist (map col-name->csv-col-name [:type-of-establishment-code    :type-of-establishment-name
                                                            :establishment-type-group-code :establishment-type-group-name])
             :dataset-name     (str (re-find #".+(?=\.csv$)" default-resource-file-name) "-establishment-types" ".csv")})
      (tc/unique-by)
      (tc/order-by [:type-of-establishment-code])
      (as-> $
          (tc/write! $ (str "./data/" (tc/dataset-name $)))))
  )
