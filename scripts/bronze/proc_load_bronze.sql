/*
========================================================================================================
Stored Procedure: Load Bronze layer(Source -> Bronze)
========================================================================================================
Script purpouse:
    This stored Procedure loads data into the 'bronze' from the external CSV files.
    It performs the following actions:
    -Truncates the bronze tables before loading data.
    -uses the Bulk insert command to load data from csv files to brone tables.

pararmeters:
None.
  This stored procedure doesnt accept any parameters or return any value.

Usage Example:
    EXEC bronze.load_bronze 
=========================================================================================================
*/
Create or Alter Procedure bronze.load_bronze as 
Begin
	DECLARE  @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	Begin Try
		set @batch_start_time=GETDATE();
		print'=======================================================';
		print'loading bronze layer';
		print'=======================================================';

		print'------------------------------------------------------';
		print'loading crm tables';
		print'------------------------------------------------------';

		set @start_time=GETDATE(); 
		print'>>truncating taBLE:bronze.crm_cust_inf';
		truncate table bronze.crm_cust_info;
		print'inserting data into:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'

		set @start_time=GETDATE(); 
		print'>>truncating taBLE:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print'inserting data into:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_crm\prd_info.csv'
		with (
			 FIRSTROW =2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'

		set @start_time=GETDATE(); 
		print'>>truncating taBLE:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details
		print'inserting data into:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		From 'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_crm\sales_details.csv'
		with (
			 FIRSTROW=2,
			 FIELDTERMINATOR =',',
			 TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'

			print'------------------------------------------------------';
			print'loading erp tables';
			print'------------------------------------------------------';

		set @start_time=GETDATE(); 
		print'>>truncating taBLE:bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12
		print'inserting data into:bronze.erp_cust_az12';
		BULK INSERT  bronze.erp_cust_az12
		FROM  'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_erp\cust_az12.csv'
		with(
			 FIRSTROW =2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'


		set @start_time=GETDATE(); 
		print'>>truncating taBLE: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101
		print'inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_erp\loc_a101.csv'
		with(
			 FIRSTROW =2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'


		set @start_time=GETDATE(); 
		print'>>truncating taBLE:bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		print'inserting data into:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\nanin\OneDrive\Documents\SQL- projectsdata\source_erp\px_cat_g1v2.csv'
		with(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		set @end_time=GETDATE();
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'>>--------------------------------'
		set @batch_end_time=GETDATE();
		print'====================================='
		print'Loading Bronze layer is completed'
		print'load >>:'+ cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds'
		print'====================================='

	End Try
	Begin Catch
		print'======================================================='
		print'Error Occur During occured Loading Bronze layer'
		print'Error Message' + Error_Message();
		print'Error Message' + cast(Error_number()as nvarchar);
		print'Error Message' + cast(Error_state()as nvarchar);
		print'======================================================='
	End Catch
End



