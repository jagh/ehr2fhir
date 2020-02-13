#!/bin/bash
#
# Import a CSV into MongoDB.
#
# Parameters:
# --db <db name>: name of the MongoDB database.
# --collection <collection>: name of the collection within the database
# --file <input CSV file>: comma separated, no header line, text in double-quotes
# --fields <fields file>: list of fields (one per line) with type, e.g.: idnum.sting()
#
# Usefuls commands:
# (txt in csv)			$ cat file.txt | tr -s '[:blank:]' ',' > file.csv
# (Find & replace)		:%s/;/,/g 


# Default database
#DB=dbname

# Check parameters
while [ ! -z "$1" ]
do
  case "$1" in
    --db ) DB=$2; shift;;
    --collection ) COLLECTION=$2; shift;;
    --file ) CSV_FILE=$2; shift;;
    --fields ) FIELDS=$2; shift;;
    -h | --help ) help;;
    * ) help;;
  esac
  shift
done

if test -z "$COLLECTION"; then
    echo "You must provide a collection name with the --collection option."
    exit 1
fi
if test -z "$CSV_FILE"; then
    echo "You must provide a CSV file name with the --file option."
    exit 1
fi
if test -z "$FIELDS"; then
    echo "You must provide the list of field names and types with the --fields option."
    exit 1
fi

echo "Database: $DB"
echo "Collection: $COLLECTION"
echo "CSV file: $CSV_FILE"
echo "Fields: $FIELDS"


# Import the CSV file as a collection
mongoimport -u "user" -p "gZog46vnZXsqAzoMldg5" --db $DB --collection $COLLECTION --type csv --columnsHaveTypes --fieldFile $FIELDS --file $CSV_FILE


# Create an index on the main document key
mongo  --authenticationDatabase $DB  --username user -p gZog46vnZXsqAzoMldg5 --eval "db = db.getSiblingDB('$DB'); db.${COLLECTION}.createIndex({Idnum:1})"

