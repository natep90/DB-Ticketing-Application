import psycopg2
import pandas as pd


def getConn():
    # function to retrieve the password, construct
    # the connection string, make a connection and return it.
    pwFile = open("pw.txt", "r") # open pw file
    pw = pwFile.read() # save string in file as pw
    pwFile.close() # close pw file
    connStr = (
        "host='cmpstudb-01.cmp.uea.ac.uk' \
        dbname= 'usv20umu' \
        user='usv20umu' password = " + pw
    )
    conn = psycopg2.connect(connStr)
    return conn

def clearOutput():
    # clears the output file
    with open("output.txt", "w") as clearfile:
        clearfile.write("")


def writeOutput(output):
    # writes to the output file
    with open("output.txt", "a") as myfile:
        myfile.write(output)


try:
    # connects to DB and creates cursor object
    connection = getConn()
    cursor = connection.cursor()
    # set search path to the pirean schema
    cursor.execute("SET SEARCH_PATH TO pierian;")
    # open txt file which contains input data
    file = open("input.txt", "r")
    # clear output file ready to be written to
    clearOutput()
    # iterate through each line
    # the first character of a line denotes what sql squery should be initiated
    for line in file:
        # if the first character of a line is 'A'
        if line[0] == "A":
            # remove the 'A' itself as not relevant to the query
            # raw equals the query data
            raw = line[2:]
            # each part of the query is separated by commas so split on ','
            data = raw.split(",") 
            # try to execute/commit the sql query using the values specified in the input file      
            try:
                cursor.execute("INSERT INTO spectator (sno, sname, semail) VALUES (%s, %s, %s);", [data[0], data[1], data[2]])
                connection.commit()
                # if successful, write 'success' message to output file
                writeOutput(f"\nA. Insert Spectator {data[0]} - STATUS COMPLETE\n")
                print(f"A. Insert Spectator {data[0]} - STATUS COMPLETE\n")
            # if an exception is raised, print the error to aid in troubleshooting
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task A: {cursor.query}\n{error}\n")
                print(f"Error found during task A: {cursor.query}\n{error}\n")

        # if line starts with 'B'   
        elif line[0] == "B":
            raw = line[2:]
            data = raw.split(",")
            # try to insert events into event table and commit changes
            try:
                cursor.execute("INSERT INTO event (ecode, edesc, elocation, edate, etime, emax) VALUES (%s, %s, %s, %s, %s, %s);",
                [data[0], data[1], data[2], data[3], data[4], data[5]])
                connection.commit()
                # print 'status complete' to output.txt if insert is successful (along with the query itself)
                writeOutput(f"\nB. Insert Event {data[0]} - STATUS COMPLETE\n")
                print(f"B. Insert Event {data[0]} - STATUS COMPLETE\n")  
            # if there is an error, print error for use in debugging
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task B: {cursor.query}\n{error}\n")
                print(f"Error found during task B: {cursor.query}\n{error}\n")

        elif line[0] == "C":
            raw = line[2:]
            data = raw.split(",")
            # delete spectator with sno specified in input.txt file
            try:
                cursor.execute("DELETE FROM spectator WHERE sno=%s;" % data[0])
                connection.commit()
                writeOutput(f"\nC. Delete Spectator {data[0]} - STATUS COMPLETE \n")
                print(f"C. Delete Spectator {data[0]} - STATUS COMPLETE \n")
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task C: {cursor.query}\n{error}\n")
                print(f"Error found during task C: {cursor.query}\n{error}\n")

        elif line[0] == "D":
            raw = line[2:]
            data = raw.split(",")
            # try to delete event (and commit deletion) 
            try:
                cursor.execute("DELETE FROM event WHERE ecode='%s';" % data[0])
                connection.commit()
                writeOutput(f"\nD. Delete Event {data[0]} - STATUS COMPLETE\n")
                print(f"D. Delete Event {data[0]} - STATUS COMPLETE\n") 
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task D: {cursor.query}\n{error}\n")
                print(f"Error found during task D: {cursor.query}\n{error}\n")

        elif line[0] == "E":
            raw = line[2:]
            data = raw.split(",")
            # insert spectator into spectator table (based on values specified in input.txt)
            try:
                cursor.execute("INSERT INTO ticket (tno, ecode, sno) VALUES (%s, %s, %s);", [data[0], data[1], data[2]])
                connection.commit()
                writeOutput(f"E. Ticket {data[0]} issued\n")
                print(f"E. Ticket {data[0]} issued\n")
            # raise and print exception (if exists)
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task E: {cursor.query}\n{error}\n")
                print(f"Error found during task E: {cursor.query}\n{error}\n")

        elif line[0] == "P":
            try:
                cursor.execute("SELECT event.edate AS date, event.elocation AS location, COUNT(spectator.sno) AS expected_spectators FROM spectator\n" +
                                "INNER JOIN ticket ON spectator.sno=ticket.sno\n" +
                                "RIGHT JOIN event ON ticket.ecode=event.ecode\n" +
                                "GROUP BY event.edate, event.elocation\n" +
                                "ORDER BY event.edate, event.elocation;")
                columns = [x[0] for x in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                df = df.__repr__()
                writeOutput("P:\n\n" + df +"\n\n")
                print("P:\n\n" + df +"\n\n")
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task P: {cursor.query}\n{error}\n")
                print(f"Error found during task P: {cursor.query}\n{error}\n")
        
        elif line[0] == "Q":
            try:
                # sql to create a visual table to show expected specators at each event 
                cursor.execute("SELECT event.ecode, event.edesc AS event_description, COUNT(ticket.tno) AS tickets_issued FROM ticket\n" +
                        "FULL JOIN event ON ticket.ecode=event.ecode\n" +
                        "GROUP BY event.ecode\n" +
                        "ORDER BY event.edesc;")
                # this provides column headers for use in the pd dataframe        
                columns = [x[0] for x in cursor.description]
                # rows is the returned data from the select statment
                rows = cursor.fetchall()
                # create a dataframe (using pandas library)
                df = pd.DataFrame(rows, columns=columns)
                # dataframes cannot be used by the writeOutput function
                # therefore dataframe is converted to string and then written to output.txt
                df = df.__repr__()
                writeOutput("Q:\n\n" + df +"\n\n")
                print("Q:\n\n" + df +"\n\n")
            # print error if one exists
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task Q: {cursor.query}\n{error}\n")
                print(f"Error found during task Q: {cursor.query}\n{error}\n")
        
        elif line[0] == "R":
            raw = line[2:]
            data = raw.split(",")
            try:
                # sql to create a visual table to show expected specators at each event 
                cursor.execute("SELECT event.ecode, event.edesc AS event_description, COUNT(ticket.tno) AS tickets_issued FROM ticket\n"+
                               "FULL JOIN event ON ticket.ecode=event.ecode WHERE event.ecode='%s' GROUP BY event.ecode;" % data[0])
                # this provides column headers for use in the pd dataframe 
                columns = [x[0] for x in cursor.description]
                rows = cursor.fetchall()
                if len(rows) == 0:
                    writeOutput(f"R: No cancelled tickets for event {data[0]}\n")
                    print(f"R: No cancelled tickets for event {data[0]}\n")
                    continue
                # create a dataframe (using pandas library)
                df = pd.DataFrame(rows, columns=columns)
                # dataframes cannot be used by the writeOutput function
                # therefore dataframe is converted to string and then written to output.txt
                df = df.__repr__()
                writeOutput("Task R:\n\n" + df +"\n")
                print("Task R:\n\n" + df +"\n")
            # print error if one exists
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task R: {cursor.query}\n{error}\n")
                print(f"Error found during task R: {cursor.query}\n{error}\n")

        elif line[0] == "S":
            raw = line[2:]
            data = raw.split(",")
            try:
                # sql statement to create and then view a spectator itinerary for a given spectator (by sno)
                cursor.execute("CREATE TABLE IF NOT EXISTS spectator_itinerary AS\n"
                               "(SELECT ticket.sno AS spectator_number, spectator.sname as name, event.edesc AS event_description, "+
                               "event.elocation AS location, event.edate AS date, event.etime AS start_time FROM ticket\n"+
                               "LEFT JOIN event ON ticket.ecode=event.ecode LEFT JOIN spectator ON ticket.sno = spectator.sno WHERE spectator.sno=%s);" % data[0]+
                               "SELECT * FROM spectator_itinerary\n" +
                               "ORDER BY date, start_time;")
                connection.commit()
                # create pd dataframe and print/write to output.txt
                columns = [x[0] for x in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                df = df.__repr__()
                writeOutput(f"S: Itinerary for Spectator No. {data[0]}\n" + df +"\n")
                print(f"S: Itinerary for Spectator No. {data[0]}\n" + df +"\n")
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task S: {cursor.query}\n{error}\n")
                print(f"Error found during task S: {cursor.query}\n{error}\n")
        
        elif line[0] == "T":
            raw = line[2:]
            data = raw.split(",")
            try:
                # check ticket validity for a given ticket - returns table
                cursor.execute("SELECT * FROM check_ticket_validity(%s);" % data[0] )
                # create a dataframe to show result
                columns = [x[0] for x in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                df = df.__repr__()
                writeOutput(f"\nT:\n\n" + df)
                print(f"T:\n\n" + df)
            # print error to console and output.txt (if error)
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task T: {cursor.query}\n{error}\n")
                print(f"Error found during task T: {cursor.query}\n{error}\n")

        elif line[0] == 'V':
            raw = line[2:]
            data = raw.split(",")
            try:
                # select all cancelled tickets for given event
                cursor.execute("SELECT * FROM cancel WHERE ecode='%s';" % data[0])
                # create dataframe but check rows first
                columns = [x[0] for x in cursor.description]
                rows = cursor.fetchall()
                # if no rows, print user-friendly message stating that no cancelled tickets for given even exist
                if len(rows) == 0:
                    writeOutput(f"\nV. No cancelled tickets for event {data[0]}\n")
                    print(f"\nV. No cancelled tickets for event {data[0]}\n")
                    continue
                # otherwise, print dataframe
                df = pd.DataFrame(rows, columns=columns)
                df = df.__repr__()
                writeOutput(f"V. Cancelled tickets for event {data[0]}:\n\n" + df +"\n")
                print(f"V. Cancelled tickets for event {data[0]}:\n\n" + df +"\n")
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task V: {cursor.query}\n{error}\n")
                print(f"Error found during task V: {cursor.query}\n{error}\n")
                
    
        # close connection to DB
        elif(line[0] == 'X'):
            writeOutput("X. Pierian Games application closing down\n")
            print("X. Pierian Games application closing down")
            # close cursor object
            cursor.close()
            # close connection
            connection.close()
            # print confirm closure
            writeOutput("STATUS: COMPLETE")
            print("STATUS: COMPLETE\n")

    
        elif(line[0] == 'Z'):
            # try to truncate/drop (and commit these changes)
            try:
                cursor.execute('TRUNCATE event CASCADE; TRUNCATE spectator CASCADE; TRUNCATE ticket CASCADE;\n'+
                               'TRUNCATE cancel CASCADE; DROP TABLE IF EXISTS spectator_itinerary;')
                connection.commit()
                # print simple message to output.txt with task number (Z) and the task completed
                writeOutput("Z. Database tables emptied")
                print("Z. Database tables emptied")
            # if there is an error, print query and error to output.txt
            except(Exception, psycopg2.Error) as error:
                writeOutput(f"Error found during task Z! {error}")
                print(f"Error found during task Z! {error}")


# print error if the connection does not work
except(Exception, psycopg2.Error) as error:
    print("Error while connecting to DB", error)
# once everything else is complete, close connection (if one exists) and cursor
# print to terminal to notify user connection is terminated
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("Connection terminated")


# DEBUGGING 
# prints the sql query
# print(cur.query)

# prints all info after a query
# print(cur.description)