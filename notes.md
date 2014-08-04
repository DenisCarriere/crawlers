# Ensure Geometery Index with 2dshere

db.ottawa.ensureIndex({'loc.coordinates': '2dsphere'})

# Remove Duplicate from single column

db.ottawa.ensureIndex({location:1}, {unique: true, dropDups: true})