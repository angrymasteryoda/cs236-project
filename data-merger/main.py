import csv, os, sys

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
        getDaysPrice( header, outputData )
        file.close()
        #output to file
        with open( outputPath, 'w' ) as out:
            writer = csv.writer( out )
            writer.writerows( outputData )

def getDaysPrice( header: list, outputData: list):
    header.append( "price" )
    skip = True
    i = 0
    weekendIdx = header.index( 'stays_in_weekend_nights' )
    weekNitIdx = header.index( 'stays_in_week_nights' )
    avgPriceIdx = header.index( 'avg_price_per_room' )
    for row in outputData:
        if skip:
            skip = False
            continue
        else:
            #looking for stays_in_weekend_nights
            weekends = float( row[ weekendIdx ] )
            #looking for stays_in_week_nights
            weekdays = float( row[ weekNitIdx ] )
            #get avg price
            avgPrice = float( row[ avgPriceIdx ] )
            #sanity checks to check bad data
            if( weekends >= 0 and weekdays >= 0 ):
                price = avgPrice * ( weekdays + weekends )
            else:
                price = -1
                print( "There was a negative weekend/day value check price fields for -1's")
            row.append( round( price, 2 ) )

def main():
    DATA_DIR = '../data/'
    hotelBookPath = DATA_DIR + 'hotel-booking.csv'
    customerReservPath = DATA_DIR + "customer-reservations.csv"
    output = "merged.csv"
    
    #mutate the hotel booking csv
    changes = {
        "arrival_month": lambda value: MONTHS.index( value ) + 1
    }
    # merges = {
    #     "book_date": { "fields" : ['arrival_month', 'arrival_date_day_of_month', 'arrival_year'], "concat": "/"}
    # }
    mutateCsv( hotelBookPath, changes, {}, "hotel-booking-mutated.csv" )


    #mutate the customer csv
    changes = {
        "booking_status": lambda value: int( value == 'Canceled' )
    }
    # merges = {
    #     "book_date": { "fields" : ['arrival_month', 'arrival_date', 'arrival_year'], "concat": "/"}
    # }
    mutateCsv( customerReservPath, changes, {}, "customer-reservations-mutated.csv")
            

main()