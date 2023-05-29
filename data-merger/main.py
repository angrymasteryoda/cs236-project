import csv
import os

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

def mutateCsv( csvPath: str, changes: dict, merges: dict, outputPath: str ):
    with open( csvPath, "r" ) as file:
        header = None
        reader = csv.reader( file )
        outputData = []; #what gets put to the output file
        if header is None:
            header = next( reader )
            outputData.append( header ) #add the header to the output

        for row in reader:
            #apply changes to a csv
            for field, change in changes.items(): #do each of the changes listed in the change obj
                if field in header:
                    fieldIdx = header.index( field )
                    row[ fieldIdx ] = change( row[ fieldIdx ] ) #convert the value with the lambda's

            #merge together columns into a new column
            for field, change in merges.items():
                mergeFields = change.get( 'fields' )
                concat = change.get( "concat", "" ) #get concat or blank
                #get me all the values of the items in the fields into a singluar flat array
                #the str wrap prevents concat with int later on dont need it if only doing strings
                val = [ str( row[ header.index( x ) ] ) for x in mergeFields if x in header]
                if field not in header:
                    header.append( field ) #add the new fields to the header array if it doesnt exist
                row.append( concat.join( val ) ) #add the new data to the end
            # print( rowCopy )
            outputData.append( row )
        file.close()
        #output to file
        with open( outputPath, 'w' ) as out:
            writer = csv.writer( out )
            writer.writerows( outputData )

# Merge an abritarily number of CSVs into one file
# Semantically, this appends all CSVs together
# Pre-conditions: All input CSVs must have the same schema.
def mergeCSV(output_csv_path: str, csv_paths: list[str]):
    error = False
    with open(output_csv_path, "w" ) as output_csv:
        writer = csv.writer(output_csv)
        global_header = None

        for csv_path in csv_paths:
            with open(csv_path, 'r') as input_csv:
                reader = csv.reader(input_csv)
                header = next(reader)
                if not global_header:
                    writer.writerow(header)
                # Check that the header of the current input CSV
                # matches the previous CSV header
                elif global_header != header:
                    print(f"Error in mergeCSV: Header for CSV '{csv_path}' doesn't match previous headers!")
                    error = True
                    break
                global_header = header

                for row in reader:
                    writer.writerow(row)
    
    if error:
        os.remove(output_csv_path)

def main():
    DATA_DIR = '../data/'
    hotelBookPath = DATA_DIR + 'hotel-booking.csv'
    customerReservPath = DATA_DIR + "customer-reservations.csv"
    output = "merged.csv"
    
    #mutate the hotel booking csv
    changes = {
        "arrival_month": lambda value: MONTHS.index( value ) + 1
    }
    merges = {
        "arrival_date": { "fields" : ['arrival_month', 'arrival_date_day_of_month', 'arrival_year'], "concat": "/"}
    }
    mutateCsv( hotelBookPath, changes, merges, "hotel-booking-mutated.csv" )

    #mutate the customer csv
    changes = {
        "booking_status": lambda value: int( value == 'Canceled' )
    }
    merges = {
        "arrival_date": { "fields" : ['arrival_month', 'arrival_date', 'arrival_year'], "concat": "/"}
    }
    mutateCsv( customerReservPath, changes, merges, "customer-reservations-mutated.csv")
            

main()