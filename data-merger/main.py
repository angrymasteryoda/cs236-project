import csv
import os

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

def mutateCsv( csvPath: str, changes: dict, merges: dict, outputPath: str ):
    with open( csvPath, "r" ) as file:
        header = None
        reader = csv.reader( file )
        outputData = [];
        if header is None:
            header = next( reader )
            outputData.append( header )
            #skip the header stuff for now
        for row in reader:
            #apply changes to a csv
            for field, change in changes.items():
                if field in header:
                    fieldIdx = header.index( field )
                    row[ fieldIdx ] = change( row[ fieldIdx ] )

            #merge together columns into a new column
            rowCopy = row.copy()
            for field, change in merges.items():
                mergeFields = change.get( 'fields' )
                concat = change.get( "concat", "" ) #get concat or blank
                val = [ str( row[ header.index( field ) ] ) for field in mergeFields if field in header]
                header.append( field )
                rowCopy.append( concat.join( val ) )
            # print( rowCopy )
            outputData.append( rowCopy )
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