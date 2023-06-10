import csv, os

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

def mutateCsv( csvPath: str, changes: dict, merges: dict, outputPath: str, prune: list | None=None ):
    with open( csvPath, "r" ) as file:
        header = None
        reader = csv.reader( file )
        outputData = [] # What gets put to the output file
        if header is None:
            header = next( reader )
            outputData.append( header ) #add the header to the output

        orig_header_len = len( header )
        # Append merge fields to header at the start
        for field in merges.keys():
            if field not in header:
                header.append(field)

        # Create a set of fields to keep
        keep_fields = set(header).difference(prune)

        for row in reader:
            #apply changes to a csv
            for field, change in changes.items(): #do each of the changes listed in the change obj
                if field in header:
                    fieldIdx = header.index( field )
                    row[ fieldIdx ] = change( row[ fieldIdx ] ) #convert the value with the lambda's

            row_data = { header[field_idx]: row[field_idx] for field_idx in range(orig_header_len) }

            # Apply a merge function to produce the merge field
            for field, merge_func in merges.items():
                merge_val = merge_func(row_data)
                row.append( merge_val )

            # Prune fields from the row
            row = [ row[field_idx] for field_idx in range(len(header)) if header[field_idx] in keep_fields ]
            outputData.append( row )

        # Remove pruned fields from header
        field_idx = 0
        while field_idx < len(header):
            if header[field_idx] not in keep_fields:
                del header[field_idx]
            else:
                field_idx += 1

        file.close()

        # Output to file
        with open( outputPath, 'w' ) as out:
            writer = csv.writer( out )
            writer.writerows( outputData )

# Merge an abritarily number of CSVs into one file
# Semantically, this appends all CSVs together
# This will be more memory hungry with since not dumping to file right away
def mergeCSV( outputPath: str, csvPaths: list[str] ):
    error = False
    mergedHeaders = []
    #open the output
    with open( outputPath, "w" ) as outCSV:      
        data = []
        for path in csvPaths: 
            try: #catch input file errors
                with open( path, "r" ) as inCSV:
                    reader = csv.DictReader( inCSV ) #read them as dicts instead of arrays
                    first = next( reader ) #get the first data row NOT the header
                    #add the header data using the real data
                    for x in first:
                        if x not in mergedHeaders:
                            mergedHeaders.append( x )

                    data.append( first ) #add the data from the that lookup
                    for row in reader: #get the rest of the file done
                        data.append( row ) 
                    inCSV.close() #clear mem
                #end with
            except: #catch input file errors
                print( "Error in mergeCSV: Error opening an input file!")
                return
        #end for
        print( f"Merged the {csvPaths} into {outputPath} with the following fields:")
        print( ", ".join( mergedHeaders) )
        writer = csv.DictWriter( outCSV, fieldnames=mergedHeaders )
        writer.writeheader() #write the headers wont by default
        writer.writerows( data ) #write all the data
        outCSV.close() #clear mem
        if error:
            os.remove( outCSV )
    
def get_days_price_per_row(row_data) -> str:
    weekends = float(row_data['stays_in_weekend_nights'])
    weeknights = float(row_data['stays_in_week_nights'])
    avg_price = float(row_data['avg_price_per_room'])

    if weekends >= 0 and weeknights >= 0:
        price = avg_price * ( weeknights + weekends )
    else:
        price = -1
        print( "There was a negative weekend/day value check price fields for -1's")
    return round( price, 2 )



def main():
    DATA_DIR = '../data/'
    hotelBookPath = DATA_DIR + 'hotel-booking.csv'
    customerReservPath = DATA_DIR + "customer-reservations.csv"
    output = "merged.csv"
    
    # Mutate the hotel booking csv
    changes = {
        "arrival_month": lambda value: MONTHS.index( value ) + 1
    }

    prune = [ 'hotel', 'arrival_date_week_number', 'email', 'avg_price_per_room', 'lead_time', 'arrival_date_day_of_month']
    mutateCsv( hotelBookPath, changes, {"price": get_days_price_per_row}, "hotel-booking-mutated.csv", prune )


    #mutate the customer csv
    changes = {
        "booking_status": lambda value: int( value == 'Canceled' )
    }
    prune = [ 'Booking_ID', 'avg_price_per_room', 'lead_time', 'arrival_date' ]
    mutateCsv( customerReservPath, changes, {"price": get_days_price_per_row}, "customer-reservations-mutated.csv", prune)
    
    mergeCSV( output, [ "hotel-booking-mutated.csv","customer-reservations-mutated.csv" ] )

main()