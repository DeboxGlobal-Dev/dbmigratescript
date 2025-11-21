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
Route::get('/hotelchainsync', [DataSyncController ::class, 'hotelChainSync']);
Route::get('/companybanksync', [DataSyncController ::class, 'companyBankSync']);
Route::get('/hotelcategorysync', [DataSyncController ::class, 'hotelCategorySync']);
Route::get('/hoteltypesync', [DataSyncController ::class, 'hotelTypeSync']);
Route::get('/syncagent', [DataSyncController ::class, 'syncAgent']);  /////check
Route::get('/syncsuppliermaster', [DataSyncController ::class, 'syncSupplierMaster']);
Route::get('/activitysync', [DataSyncController ::class, 'activitySync']);
Route::get('/transportmastersync', [DataSyncController ::class, 'transportMasterSync']);
Route::get('/hotelmealplansync', [DataSyncController ::class, 'hotelMealPlanSync']);
Route::get('/airlinemastersync', [DataSyncController ::class, 'airlineMasterSync']);
Route::get('/trainmastersync', [DataSyncController ::class, 'trainMasterSync']);
Route::get('/monumentsync', [DataSyncController ::class, 'monumentSync']);
Route::get('/hotelmastersync', [DataSyncController ::class, 'hotelMasterSync']);
Route::get('/roomtypesync', [DataSyncController ::class, 'roomTypeSync']);
Route::get('/currencymastersync', [DataSyncController ::class, 'currencyMasterSync']);
Route::get('/businesstypemastersync', [DataSyncController ::class, 'businessTypeMasterSync']);
Route::get('/seasonmastersync', [DataSyncController ::class, 'seasonMasterSync']);
Route::get('/hsnsacmasterSync', [DataSyncController ::class, 'hsnSacMasterSync']);
Route::get('/gstmastersync', [DataSyncController ::class, 'gstMasterSync']);
Route::get('/companyaddresssync', [DataSyncController ::class, 'companyAddressMasterSync']);
Route::get('/querymastersync', [DataSyncController ::class, 'queryMasterSync']);
Route::get('/guidemastersync', [DataSyncController ::class, 'guideMasterSync']);