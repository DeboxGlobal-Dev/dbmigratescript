<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Carbon\Carbon;
use Illuminate\Http\Request;

set_time_limit(0);

class DataSyncController extends Controller
{
    public function checkConnections()
    {
        $status = [];

        try {
            DB::connection('mysql')->getPdo();
            $status['mysql'] = 'Connected ✅';
        } catch (\Exception $e) {
            $status['mysql'] = $e->getMessage();
        }

        try {
            DB::connection('pgsql')->getPdo();
            $status['pgsql'] = "Connected ✅";
        } catch (\Exception $e) {
            $status['pgsql'] = $e->getMessage();
        }

        return response()->json($status);
    }

    public function syncSupplierMaster()
    {
        try {
            $mysqlUsers = DB::connection('mysql')->table('suppliersmaster')->get();

            foreach ($mysqlUsers as $data) {

                // Skip if supplier name empty
                if (empty($data->name)) continue;

                // 🔹 SupplierService JSON
                $typeColumns = [
                    'guideType',
                    'activityType',
                    'entranceType',
                    'transferType',
                    'mealType',
                    'airlinesType',
                    'trainType',
                    'visaType',
                    'otherType',
                    'companyTypeId',
                    'sightseeingType'
                ];
                $supplierService = [];
                foreach ($typeColumns as $col)
                    if (!empty($data->$col) && $data->$col > 0) $supplierService[] = (int)$data->$col;

                // 🔹 Destination JSONs
                $destinationJson = !empty($data->destinationId)
                    ? json_encode(array_map('intval', explode(',', $data->destinationId)))
                    : json_encode([]);
                $defaultDestinationJson = !empty($data->SDefultCity)
                    ? json_encode(array_map('intval', explode(',', $data->SDefultCity)))
                    : json_encode([]);

                // 🔹 Unique ID — if missing, make from MySQL ID
                $uniqueId = !empty($data->supplierNumber)
                    ? $data->supplierNumber
                    : 'S' . str_pad($data->id, 6, '0', STR_PAD_LEFT);

                // 🔹 Common record
                $record = [
                    'Name'                => $data->name,
                    'AliasName'           => $data->aliasname,
                    'PanInformation'      => $data->panInformation,
                    'SupplierService'     => json_encode($supplierService),
                    'Destination'         => $destinationJson,
                    'PaymentTerm'         => $data->paymentTerm == 1 ? 'Cash' : ($data->paymentTerm == 2 ? 'Credit' : null),
                    'ConfirmationType'    => $data->confirmationStatus == 3 ? 'Manual' : ($data->confirmationStatus == 6 ? 'Auto' : null),
                    'LocalAgent'          => $data->isLocalAgent == 1 ? 'Yes' : 'No',
                    'Agreement'           => $data->agreement == 1 ? 'Yes' : ($data->agreement == 0 ? 'No' : null),
                    'Status'              => $data->status == 1 ? 'Yes' : ($data->status == 0 ? 'No' : null),
                    'UniqueID'            => $uniqueId,
                    'DefaultDestination'  => $defaultDestinationJson,
                    'Gst'                 => $data->gstn,
                    'Remarks'             => $data->details,
                    'updated_at'          => now(),
                    'RPK'          => $data->id,
                ];

                // 🔹 If exists (match by id), update — else insert new
                $exists = DB::connection('pgsql')->table('others.supplier')
                    ->where('id', $data->id)
                    ->exists();

                if ($exists) {
                    DB::connection('pgsql')->table('others.supplier')
                        ->where('id', $data->id)
                        ->update($record);
                } else {
                    $record['id'] = $data->id;
                    $record['created_at'] = now();
                    DB::connection('pgsql')->table('others.supplier')->insert($record);
                }
            }

            return ['status' => true, 'message' => 'Supplier Master synced successfully'];
        } catch (\Exception $e) {
            return ['status' => false, 'message' => $e->getMessage()];
        }
    }

    public function transportMasterSync()
    {
        try {
            $mysqlUsers = DB::connection('mysql')->table('packagebuildertransportmaster')->get();

            foreach ($mysqlUsers as $data) {
                // Skip if supplier name empty
                if (empty($data->transferName)) continue;


                // 🔹 Destination JSONs
                $destinationJson = !empty($data->destinationId)
                    ? json_encode(array_map('intval', explode(',', $data->destinationId)))
                    : json_encode([]);


                // 🔹 Common record
                $record = [
                    'TransferName'                => $data->transferName,
                    'Destinations'         => $destinationJson,
                    'TransferType'         => $data->transferType,
                    'Status'    => $data->status,
                    'AddedBy'             => 1,
                    'UpdatedBy'             => 1,
                    'updated_at'          => now(),
                ];

                // 🔹 If exists (match by id), update — else insert new
                $exists = DB::connection('pgsql')->table('others.transfer_master')
                    ->where('id', $data->id)
                    ->exists();

                if ($exists) {
                    DB::connection('pgsql')->table('others.transfer_master')
                        ->where('id', $data->id)
                        ->update($record);
                } else {
                    $record['RPK'] = $data->id;
                    $record['created_at'] = now();
                    DB::connection('pgsql')->table('others.transfer_master')->insert($record);
                }
            }

            return ['status' => true, 'message' => 'Tansfer Master synced successfully'];
        } catch (\Exception $e) {
            return ['status' => false, 'message' => $e->getMessage()];
        }
    }

    public function activitySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderotheractivitymaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $departmentId = null;

                if ($user->otherActivityCity) {
                    $department = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('name', $user->otherActivityCity)
                        ->first();

                    $departmentId = $department->id ?? null;
                }

                $closeDaysnameJson = !empty($user->closeDaysname)
                    ? json_encode(array_map('trim', explode(',', $user->closeDaysname)))
                    : json_encode([]);

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('sightseeing.activity_masters')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Type'           => "Activity",
                            'ServiceName'          => $user->otherActivityName,
                            'Destination'  => $departmentId,
                            'Default'  => $user->isDefault,
                            'Supplier'  => $user->supplierId,
                            'Status'  => $user->status,
                            'Description'  => $user->otherActivityDetail,
                            'RPK'  => $user->id,
                            'ClosingDay'  => $closeDaysnameJson,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Activity Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function monumentSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderentrancemaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $departmentId = null;

                if ($user->entranceCity) {
                    $department = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('name', $user->entranceCity)
                        ->first();

                    $departmentId = $department->id ?? null;
                }

                $closeDaysnameJson = !empty($user->closeDaysname)
                    ? json_encode(array_values(array_filter(
                        array_map('trim', explode(',', $user->closeDaysname)),
                        fn($v) => $v !== ""   // remove empty strings
                    )))
                    : json_encode([]);

                $uniqueId = !empty($user->id)  ? 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('sightseeing.monument_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'MonumentName'          => $user->entranceName,
                            'Destination'  => $departmentId,
                            'TransferType'  => $user->transferType,
                            'Default'  => $user->isDefault,
                            //'Supplier'  => $user->supplierId,
                            'Status'  => $user->status,
                            //'Description'  => "",
                            //'RPK'  => $user->id,
                            'JsonWeekendDays'  => $closeDaysnameJson,
                            'UniqueID'  => $uniqueId,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Monument Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function syncAgent()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('corporatemaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $name = preg_replace('/[\p{Z}\p{C}]+/u', '', $user->name);

                if (trim($name) === "") continue;

                $uniqueId = !empty($user->id)  ? 'AG' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.agent_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'WebsiteUrl'           => $user->websiteURL,
                            'CompanyName'           => $user->name ?? '',
                            'CompanyEmailAddress'          => $user->companyEmail ?? '',
                            'CompanyPhoneNumber'          => $user->companyPhone ?? '',
                            'LocalAgent'          => ($user->localAgent == 1) ? "Yes" : 'No',
                            'Category'          => $user->companyCategory,
                            'CompanyType'          => $user->companyTypeId,
                            'BussinessType'          => ($user->bussinessType) ? 1 : 0,
                            'MarketType'          => $user->marketType,
                            'Nationality'          => $user->nationality,
                            'Country'          => $user->countryId,
                            'UniqueID'          => $uniqueId,
                            'CompanyKey'          => "",
                            //'Status'          => $user->status,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Agent Data synced successfully'
            ];
        } catch (\Exception $e) {

            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function countrySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('countrymaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.country_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'ShortName'          => $user->sortname,
                            'SetDefault'  => 0,
                            'phonecode'  => $user->phonecode,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Country Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function stateSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('statemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.state_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'CountryId'          => $user->countryId,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'State Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function citySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('citymaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.city_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'StateId'          => $user->stateId,
                            'CountryId'          => $user->countryId,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'City Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function destinationSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('destinationmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $uniqueId = !empty($user->id)  ? 'S' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.destination_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'StateId'          => 0,
                            'CountryId'          => $user->countryId,
                            'UniqueID'          => $uniqueId,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Destination Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hotelChainSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('chainhotelmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                // 🔹 Destination JSONs
                $destinationJson = !empty($user->destinationId)
                    ? json_encode(array_map('intval', explode(',', $user->destinationId)))
                    : json_encode([]);

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.hotel_chain_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'HotelWebsite'          => $user->hotelwebsite,
                            'SelfSupplier'          => $user->selfsupplier,
                            'ContactType'          => $user->division,
                            'ContactName'  => $user->contactperson,
                            'ContactDesignation'  => $user->designation,
                            'ContactCountryCode'  => $user->countryCode,
                            'ContactMobile'  => $user->phone,
                            'ContactEmail'  => $user->email,
                            'Destination'  => $destinationJson,
                            'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }


            return [
                'status'  => true,
                'message' => 'Hotel Chain Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function companyBankSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('bankmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $uniqueId = !empty($user->id)  ? 'S' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.bank_master')
                    ->updateOrInsert(
                        ['RPK' => $user->id],  // Match by primary key
                        [
                            'BankName'           => $user->bankName,
                            'AccountNumber'          => $user->accountNumber,
                            'BranchAddress'          => $user->branchAddress,
                            'UpiId'          => null,
                            'AccountType'          => $user->accountType ?? '',
                            'BeneficiaryName'          => $user->beneficiaryName ?? '',
                            'BranchIfsc'          => $user->branchIFSC ?? "",
                            'BranchSwiftCode'          => $user->branchSwiftCode ?? "",
                            'currencyid'          => $user->currencyId ?? 0,
                            'purpose'          => $user->purposeRemittance ?? "",
                            'BusinessType'          => "Domestic",
                            'ShowHide'          => 1,
                            'SetDefault'          => 0,
                            'Status'  => $user->status,
                            'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Bank Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hotelCategorySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('hotelcategorymaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.hotel_category_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->hotelCategory,
                            'UploadKeyword'          => $user->uploadKeyword,
                            'Status'  => 'Active',
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Hotel Category Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hotelTypeSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('hoteltypemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.hotel_type_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'UploadKeyword'          => $user->uploadKeyword,
                            'IsHouseBoat'          => ($user->isHouseBoat == 1) ? 'Yes' : 'No',
                            'Status'  => 'Active',
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Hotel Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hotelMealPlanSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('mealplanmaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.hotel_meal_plan')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'SetDefault'  => ($user->setDefault == 1) ? 'Yes' : 'No',
                            'Status'  => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Hotel Meal Plan Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function airlineMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderairlinesmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $uniqueId = !empty($user->id)  ? 'AIR' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('air.airline_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->flightName,
                            'UniqueID'  => $uniqueId,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Airline Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function trainMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuildertrainsmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $uniqueId = !empty($user->id)  ? 'TRAI' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('train.train_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->trainName,
                            'UniqueID'  => $uniqueId,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Train Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hotelMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderhotelmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                $hotelCityId = null;
                if ($user->hotelCity) {
                    $department = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('name', $user->hotelCity)
                        ->first();

                    $hotelCityId = $department->id ?? null;
                }

                $countryId = null;
                if ($user->hotelCountry) {
                    $countrydata = DB::connection('mysql')
                        ->table('countrymaster')
                        ->where('name', $user->hotelCountry)
                        ->first();

                    $countryId = $countrydata->id ?? null;
                }

                // 🔹 Unique ID — if missing, make from MySQL ID
                $uniqueId = !empty($user->id)  ? 'HOTL' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // 🔹 Build Hotel Basic Details JSON
                $hotelBasicDetails = [
                    "Verified"        => (int)($user->verified ?? 0),
                    "HotelGSTN"       => $user->gstn ?? "",
                    "HotelInfo"       => $user->hotelInfo ?? "",
                    "HotelLink"       => $user->hoteldetail ?? "",
                    "HotelType"       => (int)($user->hotelTypeId ?? 0),
                    "HotelChain"      => (int)($user->hotelChain ?? 0),
                    "CheckInTime"     => $user->checkInTime ?? "",
                    "HotelPolicy"     => $user->policy ?? "",
                    "CheckOutTime"    => $user->checkOutTime ?? "",
                    "HotelAddress"    => $user->hotelAddress ?? "",
                    "InternalNote"    => $user->internalNote ?? "",
                    "HotelCategory"   => (int)($user->hotelCategoryId ?? 0),
                    // Convert comma-separated room IDs to array
                    "HotelRoomType"   => !empty($user->roomType)
                        ? array_values(array_filter(
                            array_map('trim', explode(',', $user->roomType)),
                            fn($v) => $v !== ""
                        ))
                        : [],

                    "HotelAmenities"  => $user->amenities ?? ""
                ];

                $hotelBasicDetailsJson = json_encode($hotelBasicDetails);

                // FETCH HOTEL CONTACT DETAILS FROM MYSQL
                $hotelContacts = DB::connection('mysql')
                    ->table('hotelcontactpersonmaster')  // <-- Change to your correct table name
                    ->where('corporateId', $user->id)
                    ->get();

                // FORMAT CONTACT DETAILS AS JSON
                $contactDetailsArray = [];

                foreach ($hotelContacts as $c) {
                    $contactDetailsArray[] = [
                        "Division"       => $c->division,
                        "NameTitle"      => $c->nameTitle,
                        "FirstName"      => $c->firstName,
                        "LastName"       => $c->lastName,
                        "Designation"    => $c->designation,
                        "CountryCode"    => $c->countryCode,
                        "Phone1"         => $c->phone,
                        "Phone2"         => $c->phone2,
                        "Phone3"         => $c->phone3,
                        "Email"          => $c->email,
                        "SecondaryEmail" => $c->email2,
                    ];
                }

                // Convert to JSON (empty array if no contacts)
                $hotelContactJson = json_encode($contactDetailsArray, JSON_UNESCAPED_UNICODE);

                $rateRows  = DB::connection('mysql')
                    ->table('dmcroomtariff')
                    ->where('serviceid', $user->id) // serviceid = HotelId
                    ->get();

                // If no rate found, store empty array
                if ($rateRows->isEmpty()) {
                    $rateJson = json_encode([]);
                } else {

                    // Fetch destination name (already mapping HotelCityId above)
                    $destination = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('id', $hotelCityId)
                        ->first();

                    $destinationName = $destination->name ?? "";

                    $hotelCategoryName = null;
                    if (!empty($user->roomType)) {
                        $hotelCategoryData = DB::connection('mysql')
                            ->table('hotelcategorymaster')
                            ->where('id', $user->hotelCategoryId)
                            ->first();

                        $hotelCategoryName = $hotelCategoryData->name ?? null;  // Use the correct column name
                    }

                    $hotelTypeName = null;
                    if (!empty($user->roomType)) {
                        $hotelTypeData = DB::connection('mysql')
                            ->table('hoteltypemaster')
                            ->where('id', $user->hotelTypeId)
                            ->first();

                        $hotelTypeName = $hotelTypeData->hotelCategory ?? null;  // Use the correct column name
                    }

                    // HEADER (Static Structure)
                    $header = [
                        "RateChangeLog" => [
                            [
                                "ChangeDateTime" => "",
                                "ChangedByID" => "",
                                "ChangeByValue" => "",
                                "ChangeSetDetail" => [
                                    [
                                        "ChangeFrom" => "",
                                        "ChangeTo" => ""
                                    ]
                                ]
                            ]
                        ]
                    ];

                    $rateDetailsList = [];

                    foreach ($rateRows as $rr) {

                        $supplierName = null;
                        if (!empty($rr->supplierId)) {
                            $supplierData = DB::connection('mysql')
                                ->table('suppliersmaster')
                                ->where('id', $rr->supplierId)
                                ->first();

                            $supplierName = $supplierData->name ?? null;  // Use the correct column name
                        }

                        $roomTypeName = null;
                        if (!empty($rr->roomType)) {
                            $supplierData = DB::connection('mysql')
                                ->table('roomtypemaster')
                                ->where('id', $rr->roomType)
                                ->first();

                            $roomTypeName = $supplierData->name ?? null;  // Use the correct column name
                        }

                        $mealPlanName = null;
                        if (!empty($rr->roomType)) {
                            $mealPlanData = DB::connection('mysql')
                                ->table('mealplanmaster')
                                ->where('id', $rr->mealPlan)
                                ->first();

                            $mealPlanName = $mealPlanData->name ?? null;  // Use the correct column name
                        }

                        // Room Bed Type Example → you can modify if beds differ
                        $roomBedType = [
                            [
                                "RoomBedTypeId" => 3,
                                "RoomBedTypeName" => "SGL Room",
                                "RoomCost" => (float)$rr->singleoccupancy,
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => (float)$rr->singleoccupancy
                            ],
                            [
                                "RoomBedTypeId" => 4,
                                "RoomBedTypeName" => "DBL Room",
                                "RoomCost" => (float)$rr->doubleoccupancy,
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => (float)$rr->doubleoccupancy
                            ],
                            [
                                "RoomBedTypeId" => 5,
                                "RoomBedTypeName" => "TWIN Room",
                                "RoomCost" => 0,  // If no twin column, set 0
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => 0
                            ],
                            [
                                "RoomBedTypeId" => 6,
                                "RoomBedTypeName" => "TPL Room",
                                "RoomCost" => (float)$rr->tripleoccupancy,
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => (float)$rr->tripleoccupancy
                            ],
                            [
                                "RoomBedTypeId" => 7,
                                "RoomBedTypeName" => "ExtraBed(A)",
                                "RoomCost" => (float)$rr->extraBed,
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => (float)$rr->extraBed
                            ],
                            [
                                "RoomBedTypeId" => 8,
                                "RoomBedTypeName" => "ExtraBed(C)",
                                "RoomCost" => (float)$rr->childwithextrabed,
                                "RoomTaxValue" => "0%",
                                "RoomCostRateValue" => 0,
                                "RoomTotalCost" => (float)$rr->childwithextrabed
                            ],
                        ];


                        //mealType
                        $mealTypes = [
                            [
                                "MealTypeId"        => 1,
                                "MealCost"          => (float)$rr->breakfast,
                                "MealTypeName"      => "Breakfast",
                                "MealTaxSlabName"   => "IT",
                                "MealTaxValue"      => 0,
                                "MealCostRateValue" => 0,
                                "MealTotalCost"     => (float)$rr->breakfast
                            ],
                            [
                                "MealTypeId"        => 3,
                                "MealCost"          => (float)$rr->lunch,
                                "MealTypeName"      => "Lunch",
                                "MealTaxSlabName"   => "IT",
                                "MealTaxValue"      => 0,
                                "MealCostRateValue" => 0,
                                "MealTotalCost"     => (float)$rr->lunch
                            ],
                            [
                                "MealTypeId"        => 2,
                                "MealCost"          => (float)$rr->dinner,
                                "MealTypeName"      => "Dinner",
                                "MealTaxSlabName"   => "IT",
                                "MealTaxValue"      => 0,
                                "MealCostRateValue" => 0,
                                "MealTotalCost"     => (float)$rr->dinner
                            ]
                        ];

                        $ssid = \Illuminate\Support\Str::uuid()->toString();
                        $rateDetailsList[] = [
                            "UniqueID" => $ssid,
                            "SupplierId" => $rr->supplierId,
                            "SupplierName" => $supplierName,
                            "HotelTypeId" => $user->hotelTypeId,
                            "HotelTypeName" => $hotelTypeName,
                            "HotelCategoryId" => $user->hotelCategoryId,
                            "HotelCategoryName" => $hotelCategoryName,
                            "ValidFrom" => $rr->fromDate,
                            "ValidTo" => $rr->toDate,
                            "MarketTypeId" => (int)$rr->marketType,
                            "MarketTypeName" => "",
                            "PaxTypeId" => (int)$rr->paxType,
                            "PaxTypeName" => "",
                            "TarrifeTypeId" => (int)$rr->tarifType,
                            "TarrifeTypeName" => "",
                            "HotelChainId" => "",
                            "HotelChainName" => "",
                            "UserId" => "",
                            "UserName" => "",
                            "SeasonTypeID" => (int)$rr->seasonType,
                            "SeasonTypeName" => "",
                            "SeasonYear" => $rr->seasonYear,
                            "WeekendDays" => null,
                            "WeekendDaysName" => null,
                            "DayList" => [],
                            "RoomTypeId" => (int)$rr->roomType,
                            "RoomTypeName" => $roomTypeName,
                            "MealPlanId" => $rr->mealPlan,
                            "MealPlanName" => $mealPlanName,
                            "CurrencyId" => (int)$rr->currencyId,
                            "CurrencyName" => "INR",
                            "CurrencyConversionRate" => "",
                            "RoomTaxSlabId" => "",
                            "RoomTaxSlabValue" => "",
                            "RoomTaxSlabName" => "",
                            "MealTaxSlabId" => "",
                            "MealTaxSlabName" => "",
                            "MealTaxSlabValue" => "",
                            "MealType" => $mealTypes,
                            "TAC" => $rr->roomTAC,
                            "RoomBedType" => $roomBedType,
                            "MarkupType" => $rr->markupType,
                            "MarkupCost" => "",
                            "TotalCost" => number_format(($rr->roomprice + ($rr->breakfast + $rr->lunch + $rr->dinner)), 2, '.', ''),
                            "GrandTotal" => number_format(($rr->roomprice + ($rr->breakfast + $rr->lunch + $rr->dinner)), 2, '.', ''),
                            "RoomTotalCost" => number_format($rr->roomprice, 2, '.', ''),
                            "MealTotalCost" => number_format($rr->breakfast + $rr->lunch + $rr->dinner, 2, '.', ''),
                            "Remarks" => $rr->remarks,
                            "Status" => 'Active',
                            "BlackoutDates" => [],
                            "GalaDinner" => [],
                        ];
                    }


                    $rateStructure = [
                        "HotelId" => $user->id,
                        "HotelUUID" => $uniqueId,
                        "HotelName" => $user->hotelName,
                        "DestinationID" => $hotelCityId,
                        "DestinationName" => $destinationName,
                        "Header" => $header,
                        "Data" => [
                            [
                                "Total" => count($rateDetailsList),
                                "RateDetails" => $rateDetailsList
                            ]
                        ]
                    ];

                    $rateJson = json_encode($rateStructure, JSON_UNESCAPED_UNICODE);

                    // Only run if rateDetailsList has data
                    if (!empty($rateDetailsList)) {
                        foreach ($rateDetailsList as $rateItem) {
                            // Extract dates
                            $startDate = Carbon::parse($rateItem['ValidFrom']);
                            $endDate   = Carbon::parse($rateItem['ValidTo']);

                            $destinationUniqueID = !empty($hotelCityId)  ? 'DES' . str_pad($hotelCityId, 6, '0', STR_PAD_LEFT) : '';
                            $supplierUniqueID = !empty($rateItem['SupplierId'])  ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT) : '';

                            // Loop day-by-day
                            while ($startDate->lte($endDate)) {

                                DB::connection('pgsql')
                                    ->table('hotel.hotel_search')
                                    ->updateOrInsert(
                                        [
                                            "ServiceRateUniqueId" => $rateItem['UniqueID'],  // unique per rate
                                            "HotelID"             => $uniqueId,
                                            "date"                => $startDate->format("Y-m-d")
                                        ],
                                        [
                                            "DestinationID" => $destinationUniqueID,
                                            //"RoomBedType"   => json_encode($rateItem['RoomBedType'], JSON_UNESCAPED_UNICODE),
                                            "SupplierID"    => $supplierUniqueID,
                                            "CompanyID"     => 0,
                                            "CurrencyID"    => $rateItem['CurrencyId'],
                                            "RateJson"      => $rateJson,
                                            "Status"        => "Active",
                                            "AddedBy"       => 1,
                                            "UpdatedBy"     => 1,
                                            "created_at"    => now(),
                                            "updated_at"    => now()
                                        ]
                                    );

                                $startDate->addDay(); // next date
                            }
                        }
                    }
                }

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.hotel_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'HotelName'          => $user->hotelName,
                            'SelfSupplier'  => $user->supplier,
                            'HotelCountry'  => $countryId,
                            'HotelCity'  => $hotelCityId,
                            'HotelBasicDetails'  => $hotelBasicDetailsJson,
                            'HotelContactDetails'  => $hotelContactJson,
                            'RateJson'  => $rateJson,
                            'UniqueID'  => $uniqueId,
                            'Destination'  => $hotelCityId,
                            'default'  => 'No',
                            'SupplierId'  => $user->supplierId,
                            'HotelTypeId'  => $user->hotelTypeId,
                            'HotelAddress'  => $user->hotelAddress,
                            'HotelCategory'  => $user->hotelCategoryId,
                            //'Status'  => ($user->status == 1) ? 'Active' : 'Inactive',
                            'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Hotel Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function roomTypeSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('roomtypemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('hotel.room_type')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'Status'  => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Room Type Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function currencyMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('querycurrencymaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.currency_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'CountryId'           => $user->country,
                            'Name'           => $user->name,
                            'CountryCode'           => $user->currencyCode,
                            'ConversionRate'           => $user->currencyValue,
                            'SetDefault'           => $user->setDefault,
                            'Status'  => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Currency Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function businessTypeMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('businesstypemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.business_type_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id'           => $user->id,
                            'Name'           => $user->name,
                            'SetDefault'           => $user->setDefault,
                            'Status'  => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy'     => 1,
                            'UpdatedBy'     => 1,
                            'created_at'     => now(),
                            'updated_at'     => now(),
                        ]
                    );
            }

            return [
                'status'  => true,
                'message' => 'Business Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status'  => false,
                'message' => $e->getMessage(),
            ];
        }
    }
}
