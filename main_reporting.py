from parse_cash_flow_upload_postgres import main as upload_df
from create_views_postgres import main as cash_flow_views
from csv_upload_to_postgres import main as upload_portfolio_tables
from create_portfolio_views_postgres import main as portfolio_views
import time



start_time = time.time()

print "Uploading cash flows to database"
#upload_df()

print "Creating cash flow views"
#cash_flow_views()

print "Uploading portfolio to database"
upload_portfolio_tables()

print "Creating portfolio views"
portfolio_views()



end_time = time.time()
time_elapsed = int(end_time - start_time)
minutes, seconds = time_elapsed // 60, time_elapsed % 60

print("Total processing time is " + str(minutes) + ":" + str(seconds).zfill(2))