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