/* IC11, window setting (rule W1). Deviation: the friend set becomes W_REACH (1..2 hops), tested
   against the (friend, job, company, country) binding. */
SELECT
    Person.id,
    firstName,
    lastName,
    Company.name,
    Person_workAt_Company.workFrom
FROM
    Person,
    Person_workAt_Company,
    Company,
    Country
WHERE Person.id <> :personId
  AND Person.id = Person_workAt_Company.PersonId
  AND Person_workAt_Company.CompanyId = Company.id
  AND Person_workAt_Company.workFROM < :workFromYear
  AND Country.id = Company.LocationPlaceId
  AND Country.name = :countryName
  AND W_OK(Person, Person_workAt_Company, Company, Country)
  AND W_REACH(:personId, Person.id, 2, W_LO(Person, Person_workAt_Company, Company, Country), W_HI(Person, Person_workAt_Company, Company, Country))
ORDER BY Person_workAt_Company.workFrom, Person.id, Company.name DESC
LIMIT 10
;
