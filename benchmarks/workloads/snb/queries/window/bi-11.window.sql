/* BI11, window setting (rule W1). Deviation: the reference's three copies of Persons_of_country_w_friends are
   one pattern (a triangle of persons in the country): all nine places, three persons and three friendships
   share a valid instant (W_OK). */
SELECT count(*)
  FROM Person a, City ca, Country na, Person_knows_Person kab,
       Person b, City cb, Country nb, Person_knows_Person kbc,
       Person c, City cc, Country nc, Person_knows_Person kca
 WHERE a.LocationCityId = ca.id AND ca.PartOfCountryId = na.id AND na.name = :country
   AND b.LocationCityId = cb.id AND cb.PartOfCountryId = nb.id AND nb.name = :country
   AND c.LocationCityId = cc.id AND cc.PartOfCountryId = nc.id AND nc.name = :country
   AND kab.Person1Id = a.id AND kab.Person2Id = b.id
   AND kbc.Person1Id = b.id AND kbc.Person2Id = c.id
   AND kca.Person1Id = c.id AND kca.Person2Id = a.id
   AND a.id < b.id AND b.id < c.id
   AND :startDate <= kab.creationDate AND kab.creationDate <= :endDate
   AND :startDate <= kbc.creationDate AND kbc.creationDate <= :endDate
   AND :startDate <= kca.creationDate AND kca.creationDate <= :endDate
   AND W_OK(a, ca, na, kab, b, cb, nb, kbc, c, cc, nc, kca)
;
