<?php

use App\Http\Controllers\DataSyncController;
use Illuminate\Support\Facades\Route;

Route::get('/test', function () {
    return response()->json(['message' => 'API is working']);
});

Route::get('/check-db', [DataSyncController ::class, 'checkConnections']);

Route::get('/countrysync', [DataSyncController ::class, 'countrySync']);
Route::get('/statesync', [DataSyncController ::class, 'stateSync']);
Route::get('/citysync', [DataSyncController ::class, 'citySync']);
Route::get('/destinationsync', [DataSyncController ::class, 'destinationSync']);
Route::get('/companybanksync', [DataSyncController ::class, 'companyBankSync']);
Route::get('/currencymastersync', [DataSyncController ::class, 'currencyMasterSync']);
Route::get('/businesstypemastersync', [DataSyncController ::class, 'businessTypeMasterSync']);
Route::get('/seasonmastersync', [DataSyncController ::class, 'seasonMasterSync']);
Route::get('/hsnsacmasterSync', [DataSyncController ::class, 'hsnSacMasterSync']);
Route::get('/gstmastersync', [DataSyncController ::class, 'gstMasterSync']);
Route::get('/companyaddresssync', [DataSyncController ::class, 'companyAddressMasterSync']);
Route::get('/markettypesync', [DataSyncController ::class, 'marketTypeSync']);
Route::get('/nationalitysync', [DataSyncController ::class, 'nationalitySync']);
Route::get('/syncagent', [DataSyncController ::class, 'syncAgent']);  /////check
Route::get('/syncdirectclient', [DataSyncController ::class, 'syncDirectClient']);  /////check
Route::get('/agentcontactsync', [DataSyncController ::class, 'agentContactSync']);
Route::get('/suppliercontactsync', [DataSyncController ::class, 'supplierContactSync']);
Route::get('/syncsuppliermaster', [DataSyncController ::class, 'syncSupplierMaster']);
Route::get('/itiinfosync', [DataSyncController ::class, 'itiInfoSync']);

Route::get('/querymastersync', [DataSyncController ::class, 'queryMasterSync']);
Route::get('/invoicemastersync', [DataSyncController ::class, 'invoiceMasterSync']);

Route::get('/guidemastersync', [DataSyncController ::class, 'guideMasterSync']);

Route::get('/hotelmastersync', [DataSyncController ::class, 'hotelMasterSync']);
Route::get('/roomtypesync', [DataSyncController ::class, 'roomTypeSync']);
Route::get('/hotelmealplansync', [DataSyncController ::class, 'hotelMealPlanSync']);
Route::get('/hotelchainsync', [DataSyncController ::class, 'hotelChainSync']);
Route::get('/hotelcategorysync', [DataSyncController ::class, 'hotelCategorySync']);
Route::get('/hoteltypesync', [DataSyncController ::class, 'hotelTypeSync']);

Route::get('/vehicletypemastersync', [DataSyncController ::class, 'vehicleTypeMasterSync']);
Route::get('/transfertypesync', [DataSyncController ::class, 'transferTypeSync']);
Route::get('/transportmastersync', [DataSyncController ::class, 'transportMasterSync']);

Route::get('/airlinemastersync', [DataSyncController ::class, 'airlineMasterSync']);
Route::get('/trainmastersync', [DataSyncController ::class, 'trainMasterSync']);
Route::get('/activitysync', [DataSyncController ::class, 'activitySync']);
Route::get('/monumentsync', [DataSyncController ::class, 'monumentSync']);
