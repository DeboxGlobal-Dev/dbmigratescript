<?php

namespace App\Http\Controllers;

use Illuminate\Support\Facades\DB;
use Carbon\Carbon;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;

set_time_limit(0);
ini_set('memory_limit', '1024M');
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

    function safeTripleDecode($value)
    {
        if ($value === null || $value === '') {
            return '';
        }

        $decoded = (string) $value;

        for ($i = 0; $i < 3; $i++) {
            try {
                $tmp = base64_decode($decoded, true);

                // If decoding failed, stop
                if ($tmp === false) {
                    break;
                }

                // If re-encoding does not match, stop
                if (base64_encode($tmp) !== $decoded) {
                    break;
                }

                $decoded = $tmp;
            } catch (\Throwable $e) {
                break;
            }
        }

        // ✅ Ensure clean string (avoid DB issues)
        return trim(preg_replace('/[^\P{C}\n]+/u', '', $decoded));
    }


    // public function syncSupplierMaster()
    // {
    //     try {
    //         $mysqlUsers = DB::connection('mysql')->table('suppliersmaster')->get();


    //         foreach ($mysqlUsers as $data) {

    //             // Skip if supplier name empty
    //             if (empty($data->name)) continue;

    //             // 🔹 SupplierService JSON
    //             $typeColumns = [
    //                 'guideType',
    //                 'activityType',
    //                 'entranceType',
    //                 'transferType',
    //                 'mealType',
    //                 'airlinesType',
    //                 'trainType',
    //                 'visaType',
    //                 'otherType',
    //                 'companyTypeId',
    //                 'sightseeingType'
    //             ];


    //             $supplierService = [];
    //             foreach ($typeColumns as $col)
    //                 if (!empty($data->$col) && $data->$col > 0) $supplierService[] = (int)$data->$col;

    //             // 🔹 Destination JSONs
    //             $destinationJson = !empty($data->destinationId)
    //                 ? json_encode(array_map('intval', explode(',', $data->destinationId)))
    //                 : json_encode([]);
    //             $defaultDestinationJson = !empty($data->SDefultCity)
    //                 ? json_encode(array_map('intval', explode(',', $data->SDefultCity)))
    //                 : json_encode([]);

    //             // 🔹 Unique ID — if missing, make from MySQL ID
    //             $uniqueId = !empty($data->supplierNumber)
    //                 ? $data->supplierNumber
    //                 : 'S' . str_pad($data->id, 6, '0', STR_PAD_LEFT);

    //             // 🔹 Common record
    //             $record = [
    //                 'Name'                => $data->name,
    //                 'AliasName'           => $data->aliasname ?? '',
    //                 'PanInformation'      => $data->panInformation ?? '',
    //                 'SupplierService'     => json_encode($supplierService),
    //                 'Destination'         => $destinationJson,
    //                 'PaymentTerm'         => $data->paymentTerm == 1 ? 'Cash' : ($data->paymentTerm == 2 ? 'Credit' : null),
    //                 'ConfirmationType'    => $data->confirmationStatus == 3 ? 'Manual' : ($data->confirmationStatus == 6 ? 'Auto' : null),
    //                 'LocalAgent' => (($data->isLocalAgent ?? 0) == 1) ? 'Yes' : 'No',
    //                 'Agreement'           => $data->agreement == 1 ? 'Yes' : ($data->agreement == 0 ? 'No' : null),
    //                 'Status'              => $data->status == 1 ? 'Yes' : ($data->status == 0 ? 'No' : null),
    //                 'UniqueID'            => $uniqueId,
    //                 'DefaultDestination'  => $defaultDestinationJson,
    //                 'Gst'                 => $data->gstn ?? '',
    //                 'Remarks'             => $data->details ?? '',
    //                 'updated_at'          => now(),
    //                 'RPK'          => $data->id,
    //             ];

    //             // 🔹 If exists (match by id), update — else insert new
    //             $exists = DB::connection('pgsql')->table('others.supplier')
    //                 ->where('id', $data->id)
    //                 ->exists();

    //             if ($exists) {
    //                 DB::connection('pgsql')->table('others.supplier')
    //                     ->where('id', $data->id)
    //                     ->update($record);
    //             } else {
    //                 $record['id'] = $data->id;
    //                 $record['created_at'] = now();
    //                 DB::connection('pgsql')->table('others.supplier')->insert($record);
    //             }
    //         }

    //         return ['status' => true, 'message' => 'Supplier Master synced successfully'];
    //     } catch (\Exception $e) {
    //         return ['status' => false, 'message' => $e->getMessage()];
    //     }
    // }

    public function syncSupplierMaster()
    {
        try {

            $serviceMap = [
                'guideType' => 1,
                'restaurantType' => 2,
                'activityType' => 3,
                'transferType' => 4,
                'sightseeingType' => 5,
                'trainType' => 6,
                'airlinesType' => 9,
                'companyTypeId' => 12,
                'invoiceType' => 13,
                'otherType' => 17,
                'mealType' => 19,
                'tourPackageType' => 20,
            ];

            $mysqlUsers = DB::connection('mysql')->table('suppliersmaster')->get();

            $allDestinationIds = DB::connection('mysql')
                ->table('destinationmaster')
                ->pluck('id')
                ->map(fn($id) => (int) $id)
                ->toArray();

            foreach ($mysqlUsers as $data) {

                if (empty($data->name))
                    continue;

                /* ---------- Supplier Service ---------- */
                $supplierService = [];

                foreach ($serviceMap as $column => $serviceId) {
                    if (
                        isset($data->$column) &&
                        is_numeric($data->$column) &&
                        (int) $data->$column >= 1
                    ) {
                        $supplierService[] = $serviceId;
                    }
                }

                if (empty($supplierService)) {
                    $supplierService[] = 12; // default HOTEL
                }

                $supplierService = array_values(array_unique($supplierService));

                /* ---------- Destination ---------- */
                if ((int) ($data->isDestAll ?? 0) === 1) {

                    // isDestAll = 1 → ALL destinations
                    $destinationJson = json_encode($allDestinationIds);

                } elseif (!empty($data->destinationId)) {

                    // Specific destinations
                    $destinationJson = json_encode(
                        array_map('intval', explode(',', $data->destinationId))
                    );

                } else {

                    $destinationJson = json_encode([]);
                }

                $defaultDestinationJson = !empty($data->SDefultCity)
                    ? json_encode(array_map('intval', explode(',', $data->SDefultCity)))
                    : json_encode([]);

                /* ---------- Unique ID ---------- */
                $uniqueId = !empty($data->supplierNumber)
                    ? $data->supplierNumber
                    : 'S' . str_pad($data->id, 6, '0', STR_PAD_LEFT);

                /* ---------- Record ---------- */
                $record = [
                    'Name' => $data->name,
                    'AliasName' => $data->aliasname ?? '',
                    'PanInformation' => $data->panInformation ?? '',
                    'SupplierService' => json_encode(array_map('strval', $supplierService)),
                    'Destination' => $destinationJson,
                    'PaymentTerm' => $data->paymentTerm == 1 ? 'Cash' : ($data->paymentTerm == 2 ? 'Credit' : null),
                    'ConfirmationType' => $data->confirmationStatus == 3 ? 'Manual' : ($data->confirmationStatus == 6 ? 'Auto' : null),
                    'LocalAgent' => (($data->isLocalAgent ?? 0) == 1) ? 'Yes' : 'No',
                    'Agreement' => $data->agreement == 1 ? 'Yes' : 'No',
                    'Status' => $data->status == 1 ? 'Yes' : 'No',
                    'UniqueID' => $uniqueId,
                    'DefaultDestination' => $defaultDestinationJson,
                    'Gst' => $data->gstn ?? '',
                    'Remarks' => $data->details ?? '',
                    'RPK' => $data->id,
                    'updated_at' => now(),
                ];

                $exists = DB::connection('pgsql')
                    ->table('others.supplier')
                    ->where('id', $data->id)
                    ->exists();

                if ($exists) {
                    DB::connection('pgsql')->table('others.supplier')
                        ->where('id', $data->id)
                        ->update($record);
                } else {
                    $record['id'] = $data->id;
                    $record['created_at'] = now();
                    DB::connection('pgsql')->table('others.supplier')
                        ->insert($record);
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
                if (empty($data->transferName))
                    continue;


                // 🔹 Destination JSONs
                $destinationJson = !empty($data->destinationId)
                    ? json_encode(array_map('intval', explode(',', $data->destinationId)))
                    : json_encode([]);

                $uniqueId = 'TPT' . str_pad($data->id, 6, '0', STR_PAD_LEFT);

                // 🔹 Common record
                $record = [
                    'Name' => $data->transferName,
                    'UniqueID' => $uniqueId,
                    'DestinationId' => $destinationJson,
                    'TransferType' => $data->transferType,
                    'Status' => $data->status,
                    'AddedBy' => 1,
                    'UpdatedBy' => 1,
                    'updated_at' => now(),
                ];

                // 🔹 If exists (match by id), update — else insert new
                $exists = DB::connection('pgsql')->table('transport.transport_master')
                    ->where('id', $data->id)
                    ->exists();

                if ($exists) {
                    DB::connection('pgsql')->table('transport.transport_master')
                        ->where('id', $data->id)
                        ->update($record);
                } else {
                    //$record['RPK'] = $data->id;
                    $record['created_at'] = now();
                    DB::connection('pgsql')->table('transport.transport_master')->insert($record);
                }
            }

            return ['status' => true, 'message' => 'Tansport Master synced successfully'];
        } catch (\Exception $e) {
            return ['status' => false, 'message' => $e->getMessage()];
        }
    }

    // public function activitySync()
    // {
    //     try {
    //         // ✅ Read all data from MySQL
    //         $mysqlUsers = DB::connection('mysql')
    //             ->table('packagebuilderotheractivitymaster')
    //             ->get();

    //         foreach ($mysqlUsers as $user) {

    //             //------------------------------------
    //             // UNIQUE ID
    //             //------------------------------------
    //             $uniqueId = !empty($user->id)
    //                 ? 'ACT' . str_pad($user->id, 6, '0', STR_PAD_LEFT)
    //                 : '';

    //             $departmentId = null;

    //             if ($user->otherActivityCity) {
    //                 $department = DB::connection('mysql')
    //                     ->table('destinationmaster')
    //                     ->where('name', $user->otherActivityCity)
    //                     ->first();

    //                 $departmentId = $department->id ?? null;
    //             }

    //             $closeDaysnameJson = !empty($user->closeDaysname)
    //                 ? json_encode(array_map('trim', explode(',', $user->closeDaysname)))
    //                 : json_encode([]);

    //             // ------------------------------------------------------
    //             // ✅ FETCH ACTIVITY RATE JSON FROM dmcotheractivityrate
    //             // ------------------------------------------------------
    //             $rateData = DB::connection('mysql')
    //                 ->table('dmcotheractivityrate')
    //                 ->where('serviceid', $user->id)
    //                 ->get();


    //             //------------------------------------
    //             // HEADER
    //             //------------------------------------
    //             $header = [
    //                 "RateChangeLog" => [
    //                     [
    //                         "ChangeDateTime"   => "",
    //                         "ChangedByID"      => "",
    //                         "ChangeByValue"    => "",
    //                         "ChangeSetDetail"  => [
    //                             [
    //                                 "ChangeFrom" => "",
    //                                 "ChangeTo"   => ""
    //                             ]
    //                         ]
    //                     ]
    //                 ]
    //             ];

    //             // Build ServiceCost Array
    //             $serviceCost = [];
    //             $rateDetails = [];
    //             foreach ($rateData as $rate) {

    //                 // Supplier Name
    //                 $supplierName = "";
    //                 if (!empty($rate->supplierId)) {
    //                     $sup = DB::connection('mysql')
    //                         ->table('suppliersmaster')
    //                         ->where('id', $rate->supplierId)
    //                         ->first();

    //                     $supplierName = $sup->name ?? "";
    //                 }


    //                 $serviceCost[] = [
    //                     "UpToPax"  => $rate->maxpax ?? "",
    //                     "Rounds"   => 1,
    //                     "Class"    => 1,
    //                     "Duration" => 1,
    //                     "Amount"   => $rate->activityCost ?? "",
    //                     "Remarks"  => $rate->details ?? "",
    //                 ];

    //                 $rateUUID = \Illuminate\Support\Str::uuid()->toString();
    //                 $supplierId = $rate->supplierId ?? '';

    //                 // ------------------------------------------------------
    //                 // FINAL JSON FORMAT (NO SLASHES, VALID PGSQL JSON)
    //                 // ------------------------------------------------------
    //                 $rateDetails[]  = [
    //                     "UniqueID"        => $rateUUID,
    //                     "Type"            => "Activity",
    //                     "SupplierId"      => $rate->supplierId ?? '',
    //                     "SupplierName"    => $supplierName,
    //                     "DestinationID"   => $departmentId,
    //                     "DestinationName" => $user->otherActivityCity,
    //                     "ValidFrom"       => $rate->validFrom ?? "",
    //                     "ValidTo"         => $rate->validTo ?? "",
    //                     "Service"         => "",
    //                     "CurrencyId"      => $rate->currencyId ?? '',
    //                     "CurrencyName"    => "",
    //                     "ChildCost"       => "",
    //                     "ServiceCost"       => $serviceCost,
    //                     "TaxSlabId"       => $rate->gstTax ?? "",
    //                     "TaxSlabName"     => "",
    //                     "TaxSlabVal"      => "",
    //                     "TotalCost"       => $rate->activityCost ?? 0,
    //                     "Remarks"         => $rate->details ?? "",
    //                     "Status"          => 1,
    //                     "AddedBy"         => 1,
    //                     "UpdatedBy"       => 1,
    //                     "AddedDate"       => now(),
    //                     "UpdatedDate"     => now(),
    //                     "SupplierUID"     => "SUPP" . str_pad($supplierId, 5, '0', STR_PAD_LEFT),
    //                     "DestinationUUID" => "DEST" . str_pad($departmentId, 5, '0', STR_PAD_LEFT)
    //                 ];
    //             }

    //             //------------------------------------
    //             // BUILD RATE JSON
    //             //------------------------------------
    //             $rateJson = null;

    //             if (!empty($rateDetails)) {

    //                 $rateJsonStructure = [
    //                     "ActivityId"      => $user->id,
    //                     "ActivityUUID"    => $uniqueId,
    //                     "ActivityName"    => $user->otherActivityName,
    //                     "DestinationID"   => $departmentId,
    //                     "DestinationName" => $user->otherActivityCity,
    //                     "CompanyId"       => "",
    //                     "CompanyName"     => "",
    //                     "Header"          => $header,
    //                     "Data" => [
    //                         [
    //                             "Total"       => count($rateDetails),
    //                             "RateDetails" => $rateDetails
    //                         ]
    //                     ]
    //                 ];

    //                 $rateJson = json_encode($rateJsonStructure);

    //                 // Only run if rateDetailsList has data
    //                 if (!empty($rateDetails)) {
    //                     foreach ($rateDetails as $rateItem) {
    //                         // Extract dates
    //                         if (empty($rateItem['ValidFrom']) || empty($rateItem['ValidTo'])) {
    //                             continue; // skip only this RATE, not the activity
    //                         }

    //                         try {
    //                             $startDate = Carbon::parse($rateItem['ValidFrom']);
    //                             $endDate   = Carbon::parse($rateItem['ValidTo']);
    //                         } catch (\Exception $e) {
    //                             continue; // invalid date format
    //                         }

    //                         $destinationUniqueID = 'DES' . str_pad((int)($rateItem['DestinationID'] ?? 0), 6, '0', STR_PAD_LEFT);
    //                         $supplierUniqueID = 'SUPP' . str_pad((int)($rateItem['SupplierId'] ?? 0), 6, '0', STR_PAD_LEFT);


    //                         // Loop day-by-day
    //                         while ($startDate->lte($endDate)) {

    //                             DB::connection('pgsql')
    //                                 ->table('sightseeing.activity_search')
    //                                 ->updateOrInsert(
    //                                     [
    //                                         "RateUniqueId" => $rateItem['UniqueID'],  // unique per rate
    //                                         "ActivityUID"             => $uniqueId,
    //                                         "Date"                => $startDate->format("Y-m-d")
    //                                     ],
    //                                     [
    //                                         "Destination" => $destinationUniqueID,
    //                                         //"RoomBedType"   => json_encode($rateItem['RoomBedType'], JSON_UNESCAPED_UNICODE),
    //                                         "SupplierUID"    => $supplierUniqueID,
    //                                         "CompanyId"     => 0,
    //                                         "Currency"    => $rateItem['CurrencyId'],
    //                                         "RateJson"      => $rateJson,
    //                                         "Status"        => 1,
    //                                         "AddedBy"       => 1,
    //                                         "UpdatedBy"     => 1,
    //                                         "created_at"    => now(),
    //                                         "updated_at"    => now()
    //                                     ]
    //                                 );
    //                             ///update
    //                             $startDate->addDay(); // next date
    //                         }
    //                     }
    //                 }

    //                 // ✅ Insert / Update data to PGSQL
    //                 DB::connection('pgsql')
    //                     ->table('sightseeing.activity_masters')
    //                     ->updateOrInsert(
    //                         ['id' => $user->id],  // Match by primary key
    //                         [
    //                             'id'           => $user->id,
    //                             'Type'           => "Activity",
    //                             'ServiceName'          => $user->otherActivityName,
    //                             'Destination'  => $departmentId,
    //                             'Default'  => $user->isDefault,
    //                             'Supplier'  => $user->supplierId,
    //                             'Status'  => $user->status,
    //                             'Description'  => $user->otherActivityDetail,
    //                             'RPK'  => $user->id,
    //                             'ClosingDay'  => $closeDaysnameJson,
    //                             'UniqueID'  => $uniqueId,
    //                             'RateJson'  => $rateJson,
    //                             'AddedBy'     => 1,
    //                             'UpdatedBy'     => 1,
    //                             'created_at'     => now(),
    //                             'updated_at'     => now(),
    //                         ]
    //                     );
    //             }
    //         }

    //         return [
    //             'status'  => true,
    //             'message' => 'Activity Master Data synced successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status'  => false,
    //             'message' => $e->getMessage(),
    //         ];
    //     }
    // }

    public function activitySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderotheractivitymaster')
                ->get();


            foreach ($mysqlUsers as $user) {

                //------------------------------------
                // UNIQUE ID
                //------------------------------------
                $uniqueId = !empty($user->id)
                    ? 'ACT' . str_pad($user->id, 6, '0', STR_PAD_LEFT)
                    : '';

                //------------------------------------
                // DESTINATION
                //------------------------------------
                $departmentId = null;

                if (!empty($user->otherActivityCity)) {
                    $department = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('name', $user->otherActivityCity)
                        ->first();

                    $departmentId = $department->id ?? null;
                }

                //------------------------------------
                // CLOSING DAYS
                //------------------------------------
                $closeDaysnameJson = !empty($user->closeDaysname)
                    ? json_encode(array_map('trim', explode(',', $user->closeDaysname)))
                    : json_encode([]);

                //------------------------------------
                // FETCH RATE DATA
                //------------------------------------
                $rateData = DB::connection('mysql')
                    ->table('dmcotheractivityrate')
                    ->where('serviceid', $user->id)
                    ->get();

                //------------------------------------
                // HEADER
                //------------------------------------
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

                //------------------------------------
                // BUILD RATE DETAILS
                //------------------------------------
                $rateDetails = [];

                foreach ($rateData as $rate) {

                    // Supplier Name
                    $supplierName = "";
                    if (!empty($rate->supplierId)) {
                        $sup = DB::connection('mysql')
                            ->table('suppliersmaster')
                            ->where('id', $rate->supplierId)
                            ->first();

                        $supplierName = $sup->name ?? "";
                    }

                    $serviceCost = [
                        [
                            "UpToPax" => $rate->maxpax ?? "",
                            "Rounds" => 1,
                            "Class" => 1,
                            "Duration" => 1,
                            "Amount" => $rate->activityCost ?? "",
                            "Remarks" => $rate->details ?? "",
                        ]
                    ];

                    $rateDetails[] = [
                        "UniqueID" => \Illuminate\Support\Str::uuid()->toString(),
                        "Type" => "Activity",
                        "SupplierId" => $rate->supplierId ?? 0,
                        "SupplierName" => $supplierName,
                        "DestinationID" => $departmentId,
                        "DestinationName" => $user->otherActivityCity,
                        "ValidFrom" => $rate->validFrom ?? null,
                        "ValidTo" => $rate->validTo ?? null,
                        "CurrencyId" => $rate->currencyId ?? 0,
                        "ServiceCost" => $serviceCost,
                        "TaxSlabId" => $rate->gstTax ?? "",
                        "TotalCost" => $rate->activityCost ?? 0,
                        "Remarks" => $rate->details ?? "",
                        "Status" => 1,
                        "AddedBy" => 1,
                        "UpdatedBy" => 1,
                        "AddedDate" => now(),
                        "UpdatedDate" => now(),
                        "SupplierUID" => 'SUPP' . str_pad((int) ($rate->supplierId ?? 0), 5, '0', STR_PAD_LEFT),
                        "DestinationUUID" => 'DEST' . str_pad((int) ($departmentId ?? 0), 5, '0', STR_PAD_LEFT),
                    ];
                }

                $activityLanguageRow = DB::connection('mysql')
                    ->table('activitylanguagemaster')
                    ->where('ActivityId', $user->id)
                    ->first();


                //------------------------------------
// ACTIVITY LANGUAGE (ENGLISH ONLY)
//------------------------------------



                if ($activityLanguageRow && !empty($activityLanguageRow->lang_01)) {
                    $englishDescription = trim(
                        preg_replace(
                            '/\s+/',
                            ' ',
                            html_entity_decode(
                                strip_tags($activityLanguageRow->lang_01)
                            )
                        )
                    );
                }



                $languageJson = json_encode([
                    [
                        "LanguageId" => 1,
                        "LanguageName" => "English",
                        "LanguageDescription" => $englishDescription ?? ''
                    ],
                    [
                        "LanguageId" => 2,
                        "LanguageName" => "German",
                        "LanguageDescription" => null
                    ],
                    [
                        "LanguageId" => 4,
                        "LanguageName" => "Spanish",
                        "LanguageDescription" => null
                    ]
                ], JSON_UNESCAPED_UNICODE);


                //------------------------------------
                // BUILD RATE JSON (OPTIONAL)
                //------------------------------------
                $rateJson = null;

                if (!empty($rateDetails)) {
                    $rateJson = json_encode([
                        "ActivityId" => $user->id,
                        "ActivityUUID" => $uniqueId,
                        "ActivityName" => $user->otherActivityName,
                        "DestinationID" => $departmentId,
                        "DestinationName" => $user->otherActivityCity,
                        "Header" => $header,
                        "Data" => [
                            [
                                "Total" => count($rateDetails),
                                "RateDetails" => $rateDetails
                            ]
                        ]
                    ]);

                    //------------------------------------
                    // INSERT INTO ACTIVITY SEARCH
                    //------------------------------------
                    foreach ($rateDetails as $rateItem) {

                        if (empty($rateItem['ValidFrom']) || empty($rateItem['ValidTo'])) {
                            continue;
                        }

                        try {
                            $startDate = Carbon::parse($rateItem['ValidFrom']);
                            $endDate = Carbon::parse($rateItem['ValidTo']);
                        } catch (\Exception $e) {
                            continue;
                        }

                        while ($startDate->lte($endDate)) {

                            DB::connection('pgsql')
                                ->table('sightseeing.activity_search')
                                ->updateOrInsert(
                                    [
                                        "RateUniqueId" => $rateItem['UniqueID'],
                                        "ActivityUID" => $uniqueId,
                                        "Date" => $startDate->format('Y-m-d'),
                                    ],
                                    [
                                        "Destination" => 'DES' . str_pad((int) ($rateItem['DestinationID'] ?? 0), 6, '0', STR_PAD_LEFT),
                                        "SupplierUID" => 'SUPP' . str_pad((int) ($rateItem['SupplierId'] ?? 0), 6, '0', STR_PAD_LEFT),
                                        "CompanyId" => 0,
                                        "Currency" => $rateItem['CurrencyId'],
                                        "RateJson" => $rateJson,
                                        "Status" => 1,
                                        "AddedBy" => 1,
                                        "UpdatedBy" => 1,
                                        "created_at" => now(),
                                        "updated_at" => now(),
                                    ]
                                );

                            $startDate->addDay();
                        }
                    }
                }

                //------------------------------------
                // ✅ ALWAYS INSERT ACTIVITY MASTER
                //------------------------------------
                DB::connection('pgsql')
                    ->table('sightseeing.activity_masters')
                    ->updateOrInsert(
                        ['id' => $user->id],
                        [
                            'id' => $user->id,
                            'Type' => 'Activity',
                            'ServiceName' => $user->otherActivityName,
                            'Destination' => (int) ($departmentId ?? 0),
                            'Default' => (int) ($user->isDefault ?? 0),
                            'Supplier' => (int) ($user->supplierId ?? 0),
                            'Status' => (int) ($user->status ?? 1),

                            // ✅ STATIC LANGUAGE JSON HERE
                            'LanguageDescription' => $languageJson,

                            'RPK' => $user->id,
                            'ClosingDay' => $closeDaysnameJson,
                            'UniqueID' => $uniqueId,
                            'RateJson' => $rateJson,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );

            }

            return [
                'status' => true,
                'message' => 'Activity Master Data synced successfully',
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }


    // public function monumentSync()
    // {
    //     try {
    //         // ✅ Read all data from MySQL
    //         $mysqlUsers = DB::connection('mysql')
    //             ->table('packagebuilderentrancemaster')
    //             ->get();

    //         foreach ($mysqlUsers as $user) {

    //             $destinationId = null;
    //             $destinationName = "";

    //             if ($user->entranceCity) {
    //                 $destination = DB::connection('mysql')
    //                     ->table('destinationmaster')
    //                     ->where('name', $user->entranceCity)
    //                     ->first();

    //                 $destinationId  = $destination->id ?? null;
    //                 $destinationName = $destination->name ?? "";
    //             }

    //             $closeDaysnameJson = !empty($user->closeDaysname)
    //                 ? json_encode(array_values(array_filter(
    //                     array_map('trim', explode(',', $user->closeDaysname)),
    //                     fn($v) => $v !== ""   // remove empty strings
    //                 )))
    //                 : json_encode([]);

    //             $uniqueId = !empty($user->id)  ? 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

    //             // -------------------------------
    //             // FETCH MONUMENT RATES
    //             // -------------------------------
    //             $rates = DB::connection('mysql')
    //                 ->table('dmcentrancerate')   // <-- REPLACE IF NEEDED
    //                 ->where('entranceNameId', $user->id)
    //                 ->get();

    //             // -------------------------------
    //             // BUILD HEADER
    //             // -------------------------------
    //             $header = [
    //                 "RateChangeLog" => [
    //                     [
    //                         "ChangeDateTime"   => "",
    //                         "ChangedByID"      => "",
    //                         "ChangeByValue"    => "",
    //                         "ChangeSetDetail"  => [
    //                             [
    //                                 "ChangeFrom" => "",
    //                                 "ChangeTo"   => ""
    //                             ]
    //                         ]
    //                     ]
    //                 ]
    //             ];

    //             // -------------------------------
    //         // RateDetails ARRAY
    //         // -------------------------------
    //         $rateDetails = [];

    //         foreach ($rates as $r) {

    //             // Supplier Name
    //             $supplierName = "";
    //             if (!empty($r->supplierId)) {
    //                 $sup = DB::connection('mysql')
    //                     ->table('suppliersmaster')
    //                     ->where('id', $r->supplierId)
    //                     ->first();

    //                 $supplierName = $sup->name ?? "";
    //             }

    //             // Nationality Name
    //             $nationalityName = ($r->nationality == 1) ? "Local" : "Foreign";

    //             // UUID for each rate entry
    //             $rateUUID = \Illuminate\Support\Str::uuid()->toString();

    //             $rateDetails[] = [
    //                     "UniqueID"               => $rateUUID,
    //                     "SupplierId"             => (int)$r->supplierId,
    //                     "SupplierName"           => $supplierName,
    //                     "NationalityId"          => (int)$r->nationality,
    //                     "NationalityName"        => $nationalityName,
    //                     "ValidFrom"              => $r->fromDate,
    //                     "ValidTo"                => $r->toDate,
    //                     "CurrencyId"             => (int)$r->currencyId,
    //                     "CurrencyName"           => "",
    //                     "CurrencyConversionName" => "",
    //                     "IndianAdultEntFee"      => (string)$r->adultCost,
    //                     "IndianChildEntFee"      => (string)$r->childCost,
    //                     "ForeignerAdultEntFee"   => (string)$r->adultCost,
    //                     "ForeignerChildEntFee"   => (string)$r->childCost,
    //                     "TaxSlabId"              => (int)$r->gstTax,
    //                     "TaxSlabName"            => "IT",
    //                     "TaxSlabVal"             => "0",
    //                     "TotalCost"              => 0,
    //                     "Policy"                 => "",
    //                     "TAC"                    => "",
    //                     "Remarks"                => "",
    //                     "Status"                 => (string)$r->status,
    //                     "AddedBy"                => 0,
    //                     "UpdatedBy"              => 0,
    //                     "AddedDate"              => now(),
    //                     "UpdatedDate"            => now()
    //                 ];
    //             }

    //             //----------------------------------------
    //             // BUILD FINAL RATE JSON STRUCTURE
    //             //----------------------------------------
    //             $rateJsonStructure = [
    //                 "MonumentId"      => $user->id,
    //                 "MonumentUUID"    => $uniqueId,
    //                 "MonumentName"    => $user->entranceName,
    //                 "DestinationID"   => $destinationId,
    //                 "DestinationName" => $destinationName,
    //                 "CompanyId"       => "",
    //                 "CompanyName"     => "",
    //                 "Header"          => $header,
    //                 "Data"            => [
    //                     [
    //                         "Total"       => count($rateDetails),
    //                         "RateDetails" => $rateDetails
    //                     ]
    //                 ]
    //             ];

    //             $rateJson = json_encode($rateJsonStructure, JSON_UNESCAPED_UNICODE);

    //             // ✅ Insert / Update data to PGSQL
    //             DB::connection('pgsql')
    //                 ->table('sightseeing.monument_master')
    //                 ->updateOrInsert(
    //                     ['id' => $user->id],  // Match by primary key
    //                     [
    //                         'id'           => $user->id,
    //                         'MonumentName'          => $user->entranceName,
    //                         'Destination'  => $destinationId,
    //                         'TransferType'  => $user->transferType,
    //                         'Default'  => $user->isDefault,
    //                         //'Supplier'  => $user->supplierId,
    //                         'Status'  => $user->status,
    //                         'RateJson'  =>  $rateJson,
    //                         //'RPK'  => $user->id,
    //                         'JsonWeekendDays'  => $closeDaysnameJson,
    //                         'UniqueID'  => $uniqueId,
    //                         'AddedBy'     => 1,
    //                         'UpdatedBy'     => 1,
    //                         'created_at'     => now(),
    //                         'updated_at'     => now(),
    //                     ]
    //                 );
    //         }

    //         return [
    //             'status'  => true,
    //             'message' => 'Monument Master Data synced successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status'  => false,
    //             'message' => $e->getMessage(),
    //         ];
    //     }
    // }

    //////////old working
    // public function monumentSync()
    // {
    //     try {
    //         $mysqlUsers = DB::connection('mysql')
    //             ->table('packagebuilderentrancemaster')
    //             ->get();

    //         foreach ($mysqlUsers as $user) {

    //             //------------------------------------
    //             // DESTINATION
    //             //------------------------------------
    //             $destinationId = null;
    //             $destinationName = "";

    //             if ($user->entranceCity) {
    //                 $destination = DB::connection('mysql')
    //                     ->table('destinationmaster')
    //                     ->where('name', $user->entranceCity)
    //                     ->first();

    //                 $destinationId  = $destination->id ?? null;
    //                 $destinationName = $destination->name ?? "";
    //             }

    //             //------------------------------------
    //             // CLOSE DAYS JSON
    //             //------------------------------------
    //             $closeDaysnameJson = !empty($user->closeDaysname)
    //                 ? json_encode(array_values(array_filter(
    //                     array_map('trim', explode(',', $user->closeDaysname)),
    //                     fn($v) => $v !== ""
    //                 )))
    //                 : json_encode([]);

    //             //------------------------------------
    //             // UNIQUE ID
    //             //------------------------------------
    //             $uniqueId = !empty($user->id)
    //                 ? 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT)
    //                 : '';

    //             //------------------------------------
    //             // FETCH RATES
    //             //------------------------------------
    //             $rates = DB::connection('mysql')
    //                 ->table('dmcentrancerate')
    //                 ->where('entranceNameId', $user->id)
    //                 ->get();

    //             //------------------------------------
    //             // HEADER
    //             //------------------------------------
    //             $header = [
    //                 "RateChangeLog" => [
    //                     [
    //                         "ChangeDateTime"   => "",
    //                         "ChangedByID"      => "",
    //                         "ChangeByValue"    => "",
    //                         "ChangeSetDetail"  => [
    //                             [
    //                                 "ChangeFrom" => "",
    //                                 "ChangeTo"   => ""
    //                             ]
    //                         ]
    //                     ]
    //                 ]
    //             ];

    //             //------------------------------------
    //             // BUILD RATE DETAILS (IF ANY)
    //             //------------------------------------
    //             $rateDetails = [];

    //             foreach ($rates as $r) {

    //                 // Supplier Name
    //                 $supplierName = "";
    //                 if (!empty($r->supplierId)) {
    //                     $sup = DB::connection('mysql')
    //                         ->table('suppliersmaster')
    //                         ->where('id', $r->supplierId)
    //                         ->first();

    //                     $supplierName = $sup->name ?? "";
    //                 }

    //                 // Nationality Name
    //                 $nationalityName = ($r->nationality == 1) ? "Indian" : "Foreign";

    //                 // UUID
    //                 $rateUUID = \Illuminate\Support\Str::uuid()->toString();

    //                 $rateDetails[] = [
    //                     "UniqueID"               => $rateUUID,
    //                     "SupplierId"             => (int)$r->supplierId,
    //                     "SupplierName"           => $supplierName,
    //                     "NationalityId"          => (int)$r->nationality,
    //                     "NationalityName"        => $nationalityName,
    //                     "ValidFrom"              => $r->fromDate,
    //                     "ValidTo"                => $r->toDate,
    //                     "CurrencyId"             => (int)$r->currencyId,
    //                     "CurrencyName"           => "",
    //                     "CurrencyConversionName" => "",
    //                     "IndianAdultEntFee"      => (string)$r->adultCost,
    //                     "IndianChildEntFee"      => (string)$r->childCost,
    //                     "ForeignerAdultEntFee"   => (string)$r->adultCost,
    //                     "ForeignerChildEntFee"   => (string)$r->childCost,
    //                     "TaxSlabId"              => (int)$r->gstTax,
    //                     "TaxSlabName"            => "IT",
    //                     "TaxSlabVal"             => "0",
    //                     "TotalCost"              => 0,
    //                     "Policy"                 => "",
    //                     "TAC"                    => "",
    //                     "Remarks"                => "",
    //                     "Status"                 => (string)$r->status,
    //                     "AddedBy"                => 0,
    //                     "UpdatedBy"              => 0,
    //                     "AddedDate"              => now(),
    //                     "UpdatedDate"            => now()
    //                 ];
    //             }


    //             //------------------------------------
    //             // BUILD RATE JSON ONLY IF DATA EXISTS
    //             //------------------------------------
    //             $rateJson = null;

    //             if (!empty($rateDetails)) {
    //                 $rateJsonStructure = [
    //                     "MonumentId"      => $user->id,
    //                     "MonumentUUID"    => $uniqueId,
    //                     "MonumentName"    => $user->entranceName,
    //                     "DestinationID"   => $destinationId,
    //                     "DestinationName" => $destinationName,
    //                     "CompanyId"       => "",
    //                     "CompanyName"     => "",
    //                     "Header"          => $header,
    //                     "Data"            => [
    //                         [
    //                             "Total"       => count($rateDetails),
    //                             "RateDetails" => $rateDetails
    //                         ]
    //                     ]
    //                 ];

    //                 $rateJson = json_encode($rateJsonStructure, JSON_UNESCAPED_UNICODE);

    //                 // Only run if rateDetailsList has data
    //                 if (!empty($rateDetails)) {
    //                     foreach ($rateDetails as $rateItem) {
    //                         // Extract dates
    //                         $startDate = Carbon::parse($rateItem['ValidFrom']);
    //                         $endDate   = Carbon::parse($rateItem['ValidTo']);

    //                         $destinationUniqueID = !empty($destinationId)  ? 'DES' . str_pad($destinationId, 6, '0', STR_PAD_LEFT) : '';
    //                         $supplierUniqueID = !empty($rateItem['SupplierId'])  ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT) : '';

    //                         // Loop day-by-day
    //                         while ($startDate->lte($endDate)) {

    //                             DB::connection('pgsql')
    //                                 ->table('sightseeing.monument_search')
    //                                 ->updateOrInsert(
    //                                     [
    //                                         "RateUniqueId" => $rateItem['UniqueID'],  // unique per rate
    //                                         "MonumentUID"             => $uniqueId,
    //                                         "Date"                => $startDate->format("Y-m-d")
    //                                     ],
    //                                     [
    //                                         "Destination" => $destinationUniqueID,
    //                                         //"RoomBedType"   => json_encode($rateItem['RoomBedType'], JSON_UNESCAPED_UNICODE),
    //                                         "SupplierUID"    => $supplierUniqueID,
    //                                         "CompanyId"     => 0,
    //                                         "Currency"    => $rateItem['CurrencyId'],
    //                                         "RateJson"      => $rateJson,
    //                                         "Status"        => 1,
    //                                         "AddedBy"       => 1,
    //                                         "UpdatedBy"     => 1,
    //                                         "created_at"    => now(),
    //                                         "updated_at"    => now()
    //                                     ]
    //                                 );
    //                             ///update
    //                             $startDate->addDay(); // next date
    //                         }
    //                     }
    //                 }
    //             }

    //             //------------------------------------
    //             // PREPARE INSERT DATA
    //             //------------------------------------
    //             $updateData = [
    //                 'id'             => $user->id,
    //                 'MonumentName'   => $user->entranceName,
    //                 'Destination'    => $destinationId,
    //                 'TransferType'   => $user->transferType,
    //                 'Default'        => $user->isDefault,
    //                 'Status'         => $user->status,
    //                 'JsonWeekendDays' => $closeDaysnameJson,
    //                 'UniqueID'       => $uniqueId,
    //                 'AddedBy'        => 1,
    //                 'UpdatedBy'      => 1,
    //                 'created_at'     => now(),
    //                 'updated_at'     => now(),
    //             ];

    //             // VERY IMPORTANT:
    //             // Only add RateJson if data exists
    //             if (!empty($rateJson)) {
    //                 $updateData['RateJson'] = $rateJson;
    //             }

    //             //------------------------------------
    //             // INSERT / UPDATE
    //             //------------------------------------
    //             DB::connection('pgsql')
    //                 ->table('sightseeing.monument_master')
    //                 ->updateOrInsert(
    //                     ['id' => $user->id],
    //                     $updateData
    //                 );
    //         }

    //         return [
    //             'status' => true,
    //             'message' => 'Monument Master Data synced successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status'  => false,
    //             'message' => $e->getMessage(),
    //         ];
    //     }
    // }
    ///with chunk
    // public function monumentSync()
    // {
    //     try {

    //         // ------------------------------------
    //         // Preload reference data
    //         // ------------------------------------
    //         $destinations = DB::connection('mysql')
    //             ->table('destinationmaster')
    //             ->pluck('name', 'id')
    //             ->flip(); // city => id

    //         $suppliers = DB::connection('mysql')
    //             ->table('suppliersmaster')
    //             ->pluck('name', 'id');

    //         // ------------------------------------
    //         // Process monuments in SMALL chunks
    //         // ------------------------------------
    //         DB::connection('mysql')
    //             ->table('packagebuilderentrancemaster')
    //             ->orderBy('id')
    //             ->chunkById(25, function ($users) use ($destinations, $suppliers) {

    //                 $monumentMasterRows = [];
    //                 $searchRows = [];

    //                 foreach ($users as $user) {

    //                     //------------------------------------
    //                     // DESTINATION
    //                     //------------------------------------
    //                     $destinationId   = $destinations[$user->entranceCity] ?? null;
    //                     $destinationName = $user->entranceCity ?? '';

    //                     //------------------------------------
    //                     // CLOSE DAYS JSON
    //                     //------------------------------------
    //                     $closeDaysJson = !empty($user->closeDaysname)
    //                         ? json_encode(array_values(array_filter(
    //                             array_map('trim', explode(',', $user->closeDaysname))
    //                         )))
    //                         : json_encode([]);

    //                     //------------------------------------
    //                     // UNIQUE ID
    //                     //------------------------------------
    //                     $uniqueId = 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT);

    //                     //------------------------------------
    //                     // FETCH RATES
    //                     //------------------------------------
    //                     $rates = DB::connection('mysql')
    //                         ->table('dmcentrancerate')
    //                         ->where('entranceNameId', $user->id)
    //                         ->get();

    //                     if ($rates->isEmpty()) {
    //                         continue;
    //                     }

    //                     //------------------------------------
    //                     // HEADER
    //                     //------------------------------------
    //                     $header = [
    //                         "RateChangeLog" => [
    //                             [
    //                                 "ChangeDateTime"  => "",
    //                                 "ChangedByID"     => "",
    //                                 "ChangeByValue"   => "",
    //                                 "ChangeSetDetail" => [
    //                                     ["ChangeFrom" => "", "ChangeTo" => ""]
    //                                 ]
    //                             ]
    //                         ]
    //                     ];

    //                     //------------------------------------
    //                     // RATE DETAILS
    //                     //------------------------------------
    //                     $rateDetails = [];

    //                     foreach ($rates as $r) {

    //                         $rateUUID = (string) Str::uuid();

    //                         $rateDetails[] = [
    //                             "UniqueID"        => $rateUUID,
    //                             "SupplierId"      => (int) $r->supplierId,
    //                             "SupplierName"    => $suppliers[$r->supplierId] ?? '',
    //                             "NationalityId"   => (int) $r->nationality,
    //                             "NationalityName" => $r->nationality == 1 ? "Indian" : "Foreign",
    //                             "ValidFrom"       => $r->fromDate,
    //                             "ValidTo"         => $r->toDate,
    //                             "CurrencyId"      => (int) $r->currencyId,
    //                             "IndianAdultEntFee"    => (string) $r->adultCost,
    //                             "IndianChildEntFee"    => (string) $r->childCost,
    //                             "ForeignerAdultEntFee" => (string) $r->adultCost,
    //                             "ForeignerChildEntFee" => (string) $r->childCost,
    //                             "TaxSlabId"       => (int) $r->gstTax,
    //                             "TaxSlabName"     => "IT",
    //                             "TaxSlabVal"      => "0",
    //                             "Status"          => (string) $r->status,
    //                         ];
    //                     }

    //                     //------------------------------------
    //                     // BUILD RATE JSON (ONCE)
    //                     //------------------------------------
    //                     $rateJson = json_encode([
    //                         "MonumentId"      => $user->id,
    //                         "MonumentUUID"    => $uniqueId,
    //                         "MonumentName"    => $user->entranceName,
    //                         "DestinationID"   => $destinationId,
    //                         "DestinationName" => $destinationName,
    //                         "CompanyId"       => "",
    //                         "CompanyName"     => "",
    //                         "Header"          => $header,
    //                         "Data" => [
    //                             [
    //                                 "Total" => count($rateDetails),
    //                                 "RateDetails" => $rateDetails
    //                             ]
    //                         ]
    //                     ], JSON_UNESCAPED_UNICODE);

    //                     //------------------------------------
    //                     // BUILD SEARCH ROWS (DAY-WISE)
    //                     //------------------------------------
    //                     foreach ($rateDetails as $rateItem) {

    //                         $start = Carbon::parse($rateItem['ValidFrom']);
    //                         $end   = Carbon::parse($rateItem['ValidTo']);

    //                         $destinationUID = $destinationId
    //                             ? 'DES' . str_pad($destinationId, 6, '0', STR_PAD_LEFT)
    //                             : '';

    //                         $supplierUID = $rateItem['SupplierId']
    //                             ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT)
    //                             : '';

    //                         while ($start->lte($end)) {

    //                             $searchRows[] = [
    //                                 "RateUniqueId" => $rateItem['UniqueID'],
    //                                 "MonumentUID"  => $uniqueId,
    //                                 "Date"         => $start->toDateString(),
    //                                 "Destination"  => $destinationUID,
    //                                 "SupplierUID"  => $supplierUID,
    //                                 "CompanyId"    => 0,
    //                                 "Currency"     => $rateItem['CurrencyId'],
    //                                 "RateJson"     => $rateJson,   // ✅ FIXED
    //                                 "Status"       => 1,
    //                                 "AddedBy"      => 1,
    //                                 "UpdatedBy"    => 1,
    //                                 "created_at"   => now(),
    //                                 "updated_at"   => now(),
    //                             ];

    //                             $start->addDay();
    //                         }
    //                     }

    //                     //------------------------------------
    //                     // MONUMENT MASTER
    //                     //------------------------------------
    //                     $monumentMasterRows[] = [
    //                         'id'              => $user->id,
    //                         'MonumentName'    => $user->entranceName,
    //                         'Destination'     => $destinationId,
    //                         'TransferType'    => $user->transferType,
    //                         'Default'         => $user->isDefault,
    //                         'Status'          => $user->status,
    //                         'JsonWeekendDays' => $closeDaysJson,
    //                         'UniqueID'        => $uniqueId,
    //                         'RateJson'        => $rateJson,
    //                         'AddedBy'         => 1,
    //                         'UpdatedBy'       => 1,
    //                         'created_at'      => now(),
    //                         'updated_at'      => now(),
    //                     ];
    //                 }

    //                 //------------------------------------
    //                 // BULK INSERTS
    //                 //------------------------------------
    //                 foreach (array_chunk($monumentMasterRows, 300) as $chunk) {
    //                     DB::connection('pgsql')
    //                         ->table('sightseeing.monument_master')
    //                         ->insertOrIgnore($chunk);
    //                 }

    //                 foreach (array_chunk($searchRows, 1000) as $chunk) {
    //                     DB::connection('pgsql')
    //                         ->table('sightseeing.monument_search')
    //                         ->insertOrIgnore($chunk);
    //                 }
    //             });

    //         return [
    //             'status' => true,
    //             'message' => 'Monument sync completed successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status' => false,
    //             'message' => $e->getMessage()
    //         ];
    //     }
    // }

    public function monumentSync()
    {
        try {

            /* ------------------------------------
             * PRELOAD MASTER DATA
             * ----------------------------------*/
            $destinations = DB::connection('mysql')
                ->table('destinationmaster')
                ->pluck('name', 'id')
                ->flip(); // city => id

            $suppliers = DB::connection('mysql')
                ->table('suppliersmaster')
                ->pluck('name', 'id');

            $currencies = DB::connection('mysql')
                ->table('querycurrencymaster')
                ->select('id', 'name')
                ->get()
                ->keyBy('id');

            /* ------------------------------------
             * PROCESS MONUMENTS IN CHUNKS
             * ----------------------------------*/
            DB::connection('mysql')
                ->table('packagebuilderentrancemaster')
                ->orderBy('id')
                ->chunkById(25, function ($users) use ($destinations, $suppliers, $currencies) {

                    $monumentMasterRows = [];
                    $searchRows = [];

                    foreach ($users as $user) {

                        /* ------------------------------------
                         * DESTINATION
                         * ----------------------------------*/
                        $destinationId = $destinations[$user->entranceCity] ?? null;
                        $destinationName = $user->entranceCity ?? '';

                        /* ------------------------------------
                         * UNIQUE ID
                         * ----------------------------------*/
                        $monumentUUID = 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT);

                        /* ------------------------------------
                         * FETCH RATES
                         * ----------------------------------*/
                        $rates = DB::connection('mysql')
                            ->table('dmcentrancerate')
                            ->where('entranceNameId', $user->id)
                            ->get();



                        /* ------------------------------------
                         * RATE DETAILS
                         * ----------------------------------*/
                        $rateDetails = [];

                        foreach ($rates as $r) {

                            $rateUUID = (string) Str::uuid();

                            /* ---- Currency lookup ---- */
                            $currency = $currencies[$r->currencyId] ?? null;

                            $currencyId = $currency->id ?? '';
                            $currencyName = $currency->name ?? '';
                            $conversion = $currency->conversionRate ?? 1; // fallback only

                            /* ---- Amounts ---- */
                            $adult = (float) $r->adultCost;
                            $child = (float) $r->childCost;

                            $taxValue = 0; // old DB has no %
                            $totalCost = $adult + $child;

                            $rateDetails[] = [
                                "UniqueID" => $rateUUID,

                                "SupplierId" => (int) $r->supplierId,
                                "SupplierName" => $suppliers[$r->supplierId] ?? '',

                                "ValidFrom" => $r->fromDate,
                                "ValidTo" => $r->toDate,

                                "CurrencyId" => $currencyId,
                                "CurrencyName" => $currencyName,
                                "CurrencyConversionRate" => $conversion,

                                "IndianAdultEntFee" => $adult,
                                "IndianChildEntFee" => $child,
                                "ForeignerAdultEntFee" => $adult,
                                "ForeignerChildEntFee" => 0,

                                "TaxSlabId" => (int) $r->gstTax,
                                "TaxSlabName" => "",
                                "TaxSlabValue" => $taxValue,

                                "TotalCost" => $totalCost,

                                "Policy" => "",
                                "TC" => "",
                                "TAC" => 0,
                                "Remarks" => "",

                                "Status" => "1"
                            ];
                        }

                        if (empty($rateDetails)) {
                            $rateDetails = [];
                        }
                        /* ------------------------------------
                         * BUILD RATE JSON
                         * ----------------------------------*/
                        $rateJson = json_encode([
                            "MonumentId" => $user->id,
                            "MonumentUUID" => $monumentUUID,
                            "MonumentName" => $user->entranceName,
                            "DestinationID" => $destinationId,
                            "DestinationName" => $destinationName,
                            "CompanyId" => "",
                            "CompanyName" => "",
                            "Header" => [
                                "RateChangeLog" => [
                                    [
                                        "ChangeDateTime" => "",
                                        "ChangedByID" => "",
                                        "ChangeByValue" => "",
                                        "ChangeSetDetail" => [
                                            ["ChangeFrom" => "", "ChangeTo" => ""]
                                        ]
                                    ]
                                ]
                            ],
                            "Data" => [
                                [
                                    "Total" => count($rateDetails),
                                    "RateDetails" => $rateDetails
                                ]
                            ]
                        ], JSON_UNESCAPED_UNICODE);

                        /* ------------------------------------
                         * BUILD SEARCH ROWS (DAY WISE)
                         * ----------------------------------*/
                        foreach ($rateDetails as $rateItem) {

                            $start = Carbon::parse($rateItem['ValidFrom']);
                            $end = Carbon::parse($rateItem['ValidTo']);

                            $destinationUID = $destinationId
                                ? 'DES' . str_pad($destinationId, 6, '0', STR_PAD_LEFT)
                                : '';

                            $supplierUID = $rateItem['SupplierId']
                                ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT)
                                : '';

                            while ($start->lte($end)) {

                                $searchRows[] = [
                                    "RateUniqueId" => $rateItem['UniqueID'],
                                    "MonumentUID" => $monumentUUID,
                                    "Date" => $start->toDateString(),
                                    "Destination" => $destinationUID,
                                    "SupplierUID" => $supplierUID,
                                    "CompanyId" => 0,
                                    "Currency" => $rateItem['CurrencyId'],
                                    "RateJson" => $rateJson,
                                    "Status" => 1,
                                    "AddedBy" => 1,
                                    "UpdatedBy" => 1,
                                    "created_at" => now(),
                                    "updated_at" => now(),
                                ];

                                $start->addDay();
                            }
                        }

                        /* ------------------------------------
                         * MONUMENT MASTER ROW
                         * ----------------------------------*/
                        $monumentMasterRows[] = [
                            'id' => $user->id,
                            'MonumentName' => $user->entranceName,
                            'Destination' => $destinationId,
                            'TransferType' => $user->transferType ?? 0,
                            'Default' => $user->isDefault ?? 'no',
                            'Status' => $user->status ?? 1,
                            'JsonWeekendDays' => json_encode([]),
                            'UniqueID' => $monumentUUID,
                            'RateJson' => $rateJson,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ];
                    }

                    /* ------------------------------------
                     * BULK INSERTS
                     * ----------------------------------*/
                    foreach (array_chunk($monumentMasterRows, 300) as $chunk) {
                        DB::connection('pgsql')
                            ->table('sightseeing.monument_master')
                            ->insertOrIgnore($chunk);
                    }

                    foreach (array_chunk($searchRows, 1000) as $chunk) {
                        DB::connection('pgsql')
                            ->table('sightseeing.monument_search')
                            ->insertOrIgnore($chunk);
                    }
                });

            return [
                'status' => true,
                'message' => 'Monument sync completed successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage()
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

                if (trim($name) === "")
                    continue;

                $uniqueId = !empty($user->id) ? 'AGENT' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                /**
                 * ---------------------------------------------------------
                 * ✅ Fetch fallback email & phone from contactpersonmaster
                 * ---------------------------------------------------------
                 */
                $contact = null;

                if (
                    empty($user->companyEmail) ||
                    empty($user->companyPhone)
                ) {
                    $contact = DB::connection('mysql')
                        ->table('contactpersonmaster')
                        ->where('corporateId', $user->id)
                        ->orderBy('id', 'asc') // take first contact
                        ->first();
                }

                // ✅ Final Email
                $email = !empty($user->companyEmail)
                    ? $this->safeTripleDecode($user->companyEmail)
                    : (!empty($contact->email)
                        ? $this->safeTripleDecode($contact->email)
                        : '');

                // ✅ Final Phone
                $phone = !empty($user->companyPhone)
                    ? $this->safeTripleDecode($user->companyPhone)
                    : (!empty($contact->phone)
                        ? $this->safeTripleDecode($contact->phone)
                        : '');

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.agent_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'WebsiteUrl' => $user->websiteURL ?? '',
                            'CompanyName' => $user->name ?? '',
                            'CompanyEmailAddress' => $email,
                            'CompanyPhoneNumber' => $phone,
                            'LocalAgent' => (($user->localAgent ?? null) == 1) ? 'Yes' : 'No',
                            'Category' => $user->companyCategory,
                            'CompanyType' => $user->companyTypeId,
                            'BussinessType' => 14 ?? '',
                            'MarketType' => $user->marketType ?? '',
                            'Nationality' => $user->nationality ?? '',
                            'Country' => $user->countryId,
                            'UniqueID' => $uniqueId,
                            'CompanyKey' => "",
                            'Status' => $user->status,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Agent Data synced successfully'
            ];
        } catch (\Exception $e) {

            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'ShortName' => $user->sortname,
                            'SetDefault' => 0,
                            'phonecode' => $user->phonecode,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Country Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'CountryId' => $user->countryId,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'State Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'StateId' => $user->stateId,
                            'CountryId' => $user->countryId,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'City Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function destinationSync()
    {
        try {

            // ✅ Destinations
            $destinations = DB::connection('mysql')
                ->table('destinationmaster')
                ->get();

            // ✅ Old DB language data (English description only)
            $languageRows = DB::connection('mysql')
                ->table('destinationlanguagemaster')
                ->get()
                ->keyBy('destinationId');

            foreach ($destinations as $dest) {

                // 🔹 English description from old DB
                $englishDescription = null;

                if (isset($languageRows[$dest->id])) {
                    $englishDescription = trim(
                        preg_replace(
                            '/\s+/',
                            ' ',
                            html_entity_decode(
                                strip_tags($languageRows[$dest->id]->lang_01 ?? '')
                            )
                        )
                    );
                }

                // ✅ STATIC LANGUAGE JSON (ONLY English changes)
                $languageArray = [
                    [
                        'LanguageId' => 1,
                        'LanguageName' => 'English',
                        'LanguageDescription' => $englishDescription ?: null
                    ],
                    [
                        'LanguageId' => 2,
                        'LanguageName' => 'German',
                        'LanguageDescription' => null
                    ],
                    [
                        'LanguageId' => 4,
                        'LanguageName' => 'Spanish',
                        'LanguageDescription' => null
                    ]
                ];

                $languageJson = json_encode($languageArray, JSON_UNESCAPED_UNICODE);

                $uniqueId = 'DEST' . str_pad($dest->id, 6, '0', STR_PAD_LEFT);

                DB::connection('pgsql')
                    ->table('others.destination_master')
                    ->updateOrInsert(
                        ['id' => $dest->id],
                        [
                            'id' => $dest->id,
                            'Name' => $dest->name,
                            'StateId' => $dest->stateId ?? 0,
                            'CountryId' => $dest->countryId ?? 0,
                            'UniqueID' => $uniqueId,
                            'Status' => $dest->status ?? 1,
                            'LanguageDescription' => $languageJson,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Destination Master Data synced successfully'
            ];

        } catch (\Throwable $e) {
            return [
                'status' => false,
                'message' => $e->getMessage()
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'HotelWebsite' => $user->hotelwebsite,
                            'SelfSupplier' => $user->selfsupplier,
                            'ContactType' => $user->division,
                            'ContactName' => $user->contactperson,
                            'ContactDesignation' => $user->designation,
                            'ContactCountryCode' => $user->countryCode,
                            'ContactMobile' => $user->phone,
                            'ContactEmail' => $user->email,
                            'Destination' => $destinationJson,
                            'RPK' => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }


            return [
                'status' => true,
                'message' => 'Hotel Chain Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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

                $uniqueId = !empty($user->id) ? 'S' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.bank_master')
                    ->updateOrInsert(
                        ['RPK' => $user->id],  // Match by primary key
                        [
                            'BankName' => $user->bankName,
                            'AccountNumber' => $user->accountNumber,
                            'BranchAddress' => $user->branchAddress,
                            'UpiId' => null,
                            'AccountType' => $user->accountType ?? '',
                            'BeneficiaryName' => $user->beneficiaryName ?? '',
                            'BranchIfsc' => $user->branchIFSC ?? "",
                            'BranchSwiftCode' => $user->branchSwiftCode ?? "",
                            'currencyid' => $user->currencyId ?? 0,
                            'purpose' => $user->purposeRemittance ?? "",
                            'BusinessType' => "Domestic",
                            'ShowHide' => 1,
                            'SetDefault' => 0,
                            'Status' => $user->status,
                            'RPK' => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Bank Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->hotelCategory,
                            'UploadKeyword' => $user->uploadKeyword,
                            'Status' => 'Active',
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Hotel Category Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'UploadKeyword' => $user->uploadKeyword,
                            'IsHouseBoat' => (($user->isHouseBoat ?? null) == 1) ? 'Yes' : 'No',
                            'Status' => 'Active',
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Hotel Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'SetDefault' => ($user->setDefault == 1) ? 'Yes' : 'No',
                            'Status' => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Hotel Meal Plan Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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

                $uniqueId = !empty($user->id) ? 'AIR' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('air.airline_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->flightName,
                            'UniqueID' => $uniqueId,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Airline Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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

                $uniqueId = !empty($user->id) ? 'TRAI' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('train.train_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->trainName,
                            'UniqueID' => $uniqueId,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Train Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }


    //old working code without chunk
    // public function hotelMasterSync()
    // {
    //     try {
    //         // ✅ Read all data from MySQL
    //         $mysqlUsers = DB::connection('mysql')
    //             ->table('packagebuilderhotelmaster')
    //             ->get();

    //         foreach ($mysqlUsers as $user) {

    //             $hotelCityId = null;
    //             if ($user->hotelCity) {
    //                 $department = DB::connection('mysql')
    //                     ->table('destinationmaster')
    //                     ->where('name', $user->hotelCity)
    //                     ->first();

    //                 $hotelCityId = $department->id ?? null;
    //             }

    //             $countryId = null;
    //             if ($user->hotelCountry) {
    //                 $countrydata = DB::connection('mysql')
    //                     ->table('countrymaster')
    //                     ->where('name', $user->hotelCountry)
    //                     ->first();

    //                 $countryId = $countrydata->id ?? null;
    //             }

    //             // 🔹 Unique ID — if missing, make from MySQL ID
    //             $uniqueId = !empty($user->id)  ? 'HOTL' . str_pad($user->id, 6, '0', STR_PAD_LEFT) : '';

    //             // 🔹 Build Hotel Basic Details JSON
    //             $hotelBasicDetails = [
    //                 "Verified"        => (int)($user->verified ?? 0),
    //                 "HotelGSTN"       => $user->gstn ?? "",
    //                 "HotelInfo"       => $user->hotelInfo ?? "",
    //                 "HotelLink"       => $user->hoteldetail ?? "",
    //                 "HotelType"       => (int)($user->hotelTypeId ?? 0),
    //                 "HotelChain"      => (int)($user->hotelChain ?? 0),
    //                 "CheckInTime"     => $user->checkInTime ?? "",
    //                 "HotelPolicy"     => $user->policy ?? "",
    //                 "CheckOutTime"    => $user->checkOutTime ?? "",
    //                 "HotelAddress"    => $user->hotelAddress ?? "",
    //                 "InternalNote"    => $user->internalNote ?? "",
    //                 "HotelCategory"   => (int)($user->hotelCategoryId ?? 0),
    //                 // Convert comma-separated room IDs to array
    //                 "HotelRoomType"   => !empty($user->roomType)
    //                     ? array_values(array_filter(
    //                         array_map('trim', explode(',', $user->roomType)),
    //                         fn($v) => $v !== ""
    //                     ))
    //                     : [],

    //                 "HotelAmenities"  => $user->amenities ?? ""
    //             ];

    //             $hotelBasicDetailsJson = json_encode($hotelBasicDetails);

    //             // FETCH HOTEL CONTACT DETAILS FROM MYSQL
    //             $hotelContacts = DB::connection('mysql')
    //                 ->table('hotelcontactpersonmaster')  // <-- Change to your correct table name
    //                 ->where('corporateId', $user->id)
    //                 ->get();

    //             // FORMAT CONTACT DETAILS AS JSON
    //             $contactDetailsArray = [];

    //             foreach ($hotelContacts as $c) {
    //                 $contactDetailsArray[] = [
    //                     "Division"       => $c->division ?? '',
    //                     "NameTitle"      => $c->nameTitle ?? '',
    //                     "FirstName"      => $c->firstName ?? '',
    //                     "LastName"       => $c->lastName ?? '',
    //                     "Designation"    => $c->designation ?? '',
    //                     "CountryCode"    => $c->countryCode ?? '',
    //                     "Phone1"         => $c->phone ?? '',
    //                     "Phone2"         => $c->phone2 ?? '',
    //                     "Phone3"         => $c->phone3 ?? '',
    //                     "Email"          => $c->email ?? '',
    //                     "SecondaryEmail" => $c->email2 ?? '',
    //                 ];
    //             }

    //             // Convert to JSON (empty array if no contacts)
    //             $hotelContactJson = json_encode($contactDetailsArray, JSON_UNESCAPED_UNICODE);

    //             $rateRows  = DB::connection('mysql')
    //                 ->table('dmcroomtariff')
    //                 ->where('serviceid', $user->id) // serviceid = HotelId
    //                 ->get();

    //             // If no rate found, store empty array
    //             if ($rateRows->isEmpty()) {
    //                 $rateJson = json_encode([]);
    //             } else {

    //                 // Fetch destination name (already mapping HotelCityId above)
    //                 $destination = DB::connection('mysql')
    //                     ->table('destinationmaster')
    //                     ->where('id', $hotelCityId)
    //                     ->first();

    //                 $destinationName = $destination->name ?? "";

    //                 $hotelCategoryName = null;
    //                 if (!empty($user->roomType)) {
    //                     $hotelCategoryData = DB::connection('mysql')
    //                         ->table('hotelcategorymaster')
    //                         ->where('id', $user->hotelCategoryId)
    //                         ->first();

    //                     $hotelCategoryName = $hotelCategoryData->name ?? null;  // Use the correct column name
    //                 }

    //                 $hotelTypeName = null;
    //                 if (!empty($user->roomType)) {
    //                     $hotelTypeData = DB::connection('mysql')
    //                         ->table('hoteltypemaster')
    //                         ->where('id', $user->hotelTypeId)
    //                         ->first();

    //                     $hotelTypeName = $hotelTypeData->hotelCategory ?? null;  // Use the correct column name
    //                 }

    //                 // HEADER (Static Structure)
    //                 $header = [
    //                     "RateChangeLog" => [
    //                         [
    //                             "ChangeDateTime" => "",
    //                             "ChangedByID" => "",
    //                             "ChangeByValue" => "",
    //                             "ChangeSetDetail" => [
    //                                 [
    //                                     "ChangeFrom" => "",
    //                                     "ChangeTo" => ""
    //                                 ]
    //                             ]
    //                         ]
    //                     ]
    //                 ];

    //                 $rateDetailsList = [];

    //                 foreach ($rateRows as $rr) {

    //                     $supplierName = null;
    //                     if (!empty($rr->supplierId)) {
    //                         $supplierData = DB::connection('mysql')
    //                             ->table('suppliersmaster')
    //                             ->where('id', $rr->supplierId)
    //                             ->first();

    //                         $supplierName = $supplierData->name ?? null;  // Use the correct column name
    //                     }

    //                     $roomTypeName = null;
    //                     if (!empty($rr->roomType)) {
    //                         $supplierData = DB::connection('mysql')
    //                             ->table('roomtypemaster')
    //                             ->where('id', $rr->roomType)
    //                             ->first();

    //                         $roomTypeName = $supplierData->name ?? null;  // Use the correct column name
    //                     }

    //                     $mealPlanName = null;
    //                     if (!empty($rr->roomType)) {
    //                         $mealPlanData = DB::connection('mysql')
    //                             ->table('mealplanmaster')
    //                             ->where('id', $rr->mealPlan)
    //                             ->first();

    //                         $mealPlanName = $mealPlanData->name ?? null;  // Use the correct column name
    //                     }

    //                     // Room Bed Type Example → you can modify if beds differ
    //                     $roomBedType = [
    //                         [
    //                             "RoomBedTypeId" => 3,
    //                             "RoomBedTypeName" => "SGL Room",
    //                             "RoomCost" => (float)$rr->singleoccupancy,
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => (float)$rr->singleoccupancy
    //                         ],
    //                         [
    //                             "RoomBedTypeId" => 4,
    //                             "RoomBedTypeName" => "DBL Room",
    //                             "RoomCost" => (float)$rr->doubleoccupancy,
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => (float)$rr->doubleoccupancy
    //                         ],
    //                         [
    //                             "RoomBedTypeId" => 5,
    //                             "RoomBedTypeName" => "TWIN Room",
    //                             "RoomCost" => 0,  // If no twin column, set 0
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => 0
    //                         ],
    //                         [
    //                             "RoomBedTypeId" => 6,
    //                             "RoomBedTypeName" => "TPL Room",
    //                             "RoomCost" => (float)$rr->tripleoccupancy,
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => (float)$rr->tripleoccupancy
    //                         ],
    //                         [
    //                             "RoomBedTypeId" => 7,
    //                             "RoomBedTypeName" => "ExtraBed(A)",
    //                             "RoomCost" => (float)$rr->extraBed,
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => (float)$rr->extraBed
    //                         ],
    //                         [
    //                             "RoomBedTypeId" => 8,
    //                             "RoomBedTypeName" => "ExtraBed(C)",
    //                             "RoomCost" => (float)$rr->childwithextrabed,
    //                             "RoomTaxValue" => "0%",
    //                             "RoomCostRateValue" => 0,
    //                             "RoomTotalCost" => (float)$rr->childwithextrabed
    //                         ],
    //                     ];


    //                     //mealType
    //                     $mealTypes = [
    //                         [
    //                             "MealTypeId"        => 1,
    //                             "MealCost"          => (float)$rr->breakfast,
    //                             "MealTypeName"      => "Breakfast",
    //                             "MealTaxSlabName"   => "IT",
    //                             "MealTaxValue"      => 0,
    //                             "MealCostRateValue" => 0,
    //                             "MealTotalCost"     => (float)$rr->breakfast
    //                         ],
    //                         [
    //                             "MealTypeId"        => 3,
    //                             "MealCost"          => (float)$rr->lunch,
    //                             "MealTypeName"      => "Lunch",
    //                             "MealTaxSlabName"   => "IT",
    //                             "MealTaxValue"      => 0,
    //                             "MealCostRateValue" => 0,
    //                             "MealTotalCost"     => (float)$rr->lunch
    //                         ],
    //                         [
    //                             "MealTypeId"        => 2,
    //                             "MealCost"          => (float)$rr->dinner,
    //                             "MealTypeName"      => "Dinner",
    //                             "MealTaxSlabName"   => "IT",
    //                             "MealTaxValue"      => 0,
    //                             "MealCostRateValue" => 0,
    //                             "MealTotalCost"     => (float)$rr->dinner
    //                         ]
    //                     ];


    //                     // DB::connection('pgsql')
    //                     //     ->table('others.supplier')
    //                     //     ->updateOrInsert(
    //                     //         [
    //                     //             "Name" => $user->hotelName,  // unique per rate
    //                     //             "AliasName"             => $user->hotelName,
    //                     //             "Destination"                => [$hotelCityId],
    //                     //             "SupplierService"                => [12],
    //                     //             "DefaultDestination"                => [$hotelCityId]
    //                     //         ],
    //                     //     );

    //                     $ssid = \Illuminate\Support\Str::uuid()->toString();
    //                     $rateDetailsList[] = [
    //                         "UniqueID" => $ssid,
    //                         "SupplierId" => $rr->supplierId,
    //                         "SupplierName" => $supplierName,
    //                         "HotelTypeId" => $user->hotelTypeId,
    //                         "HotelTypeName" => $hotelTypeName,
    //                         "HotelCategoryId" => $user->hotelCategoryId,
    //                         "HotelCategoryName" => $hotelCategoryName,
    //                         "ValidFrom" => $rr->fromDate,
    //                         "ValidTo" => $rr->toDate,
    //                         "MarketTypeId" => (int)$rr->marketType,
    //                         "MarketTypeName" => "",
    //                         "PaxTypeId" => (int)$rr->paxType,
    //                         "PaxTypeName" => "",
    //                         "TarrifeTypeId" => (int)$rr->tarifType,
    //                         "TarrifeTypeName" => "",
    //                         "HotelChainId" => "",
    //                         "HotelChainName" => "",
    //                         "UserId" => "",
    //                         "UserName" => "",
    //                         "SeasonTypeID" => (int)$rr->seasonType,
    //                         "SeasonTypeName" => "",
    //                         "SeasonYear" => $rr->seasonYear,
    //                         "WeekendDays" => null,
    //                         "WeekendDaysName" => null,
    //                         "DayList" => [],
    //                         "RoomTypeId" => (int)$rr->roomType,
    //                         "RoomTypeName" => $roomTypeName,
    //                         "MealPlanId" => $rr->mealPlan,
    //                         "MealPlanName" => $mealPlanName,
    //                         "CurrencyId" => (int)$rr->currencyId,
    //                         "CurrencyName" => "INR",
    //                         "CurrencyConversionRate" => "",
    //                         "RoomTaxSlabId" => "",
    //                         "RoomTaxSlabValue" => "",
    //                         "RoomTaxSlabName" => "",
    //                         "MealTaxSlabId" => "",
    //                         "MealTaxSlabName" => "",
    //                         "MealTaxSlabValue" => "",
    //                         "MealType" => $mealTypes,
    //                         "TAC" => $rr->roomTAC,
    //                         "RoomBedType" => $roomBedType,
    //                         "MarkupType" => $rr->markupType,
    //                         "MarkupCost" => "",
    //                         "TotalCost" => number_format(($rr->roomprice + ($rr->breakfast + $rr->lunch + $rr->dinner)), 2, '.', ''),
    //                         "GrandTotal" => number_format(($rr->roomprice + ($rr->breakfast + $rr->lunch + $rr->dinner)), 2, '.', ''),
    //                         "RoomTotalCost" => number_format($rr->roomprice, 2, '.', ''),
    //                         "MealTotalCost" => number_format($rr->breakfast + $rr->lunch + $rr->dinner, 2, '.', ''),
    //                         "Remarks" => $rr->remarks,
    //                         "Status" => 'Active',
    //                         "BlackoutDates" => [],
    //                         "GalaDinner" => [],
    //                     ];
    //                 }


    //                 $rateStructure = [
    //                     "HotelId" => $user->id,
    //                     "HotelUUID" => $uniqueId,
    //                     "HotelName" => $user->hotelName,
    //                     "DestinationID" => $hotelCityId,
    //                     "DestinationName" => $destinationName,
    //                     "Header" => $header,
    //                     "Data" => [
    //                         [
    //                             "Total" => count($rateDetailsList),
    //                             "RateDetails" => $rateDetailsList
    //                         ]
    //                     ]
    //                 ];

    //                 $rateJson = json_encode($rateStructure);

    //                 // Only run if rateDetailsList has data
    //                 if (!empty($rateDetailsList)) {
    //                     foreach ($rateDetailsList as $rateItem) {
    //                         // Extract dates
    //                         $startDate = Carbon::parse($rateItem['ValidFrom']);
    //                         $endDate   = Carbon::parse($rateItem['ValidTo']);

    //                         $destinationUniqueID = !empty($hotelCityId)  ? 'DES' . str_pad($hotelCityId, 6, '0', STR_PAD_LEFT) : '';
    //                         $supplierUniqueID = !empty($rateItem['SupplierId'])  ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT) : '';

    //                         // Loop day-by-day
    //                         while ($startDate->lte($endDate)) {

    //                             DB::connection('pgsql')
    //                                 ->table('hotel.hotel_search')
    //                                 ->updateOrInsert(
    //                                     [
    //                                         "ServiceRateUniqueId" => $rateItem['UniqueID'],  // unique per rate
    //                                         "HotelID"             => $uniqueId,
    //                                         "date"                => $startDate->format("Y-m-d")
    //                                     ],
    //                                     [
    //                                         "DestinationID" => $destinationUniqueID,
    //                                         //"RoomBedType"   => json_encode($rateItem['RoomBedType'], JSON_UNESCAPED_UNICODE),
    //                                         "SupplierID"    => $supplierUniqueID,
    //                                         "CompanyID"     => 0,
    //                                         "CurrencyID"    => $rateItem['CurrencyId'],
    //                                         "RateJson"      => $rateJson,
    //                                         "Status"        => "Active",
    //                                         "AddedBy"       => 1,
    //                                         "UpdatedBy"     => 1,
    //                                         "created_at"    => now(),
    //                                         "updated_at"    => now()
    //                                     ]
    //                                 );
    //                             ///update
    //                             $startDate->addDay(); // next date
    //                         }
    //                     }
    //                 }
    //             }

    //             // ✅ Insert / Update data to PGSQL
    //             DB::connection('pgsql')
    //                 ->table('hotel.hotel_master')
    //                 ->updateOrInsert(
    //                     ['id' => $user->id],  // Match by primary key
    //                     [
    //                         'id'           => $user->id,
    //                         'HotelName'          => $user->hotelName,
    //                         'SelfSupplier'  => $user->supplier,
    //                         'HotelCountry'  => $countryId,
    //                         'HotelCity'  => $hotelCityId,
    //                         'HotelBasicDetails'  => $hotelBasicDetailsJson,
    //                         'HotelContactDetails'  => $hotelContactJson,
    //                         'RateJson'  => $rateJson,
    //                         'UniqueID'  => $uniqueId,
    //                         'Destination'  => $hotelCityId,
    //                         'default'  => 'No',
    //                         'SupplierId'  => $user->supplierId,
    //                         'HotelTypeId'  => $user->hotelTypeId,
    //                         'HotelAddress'  => $user->hotelAddress,
    //                         'HotelCategory'  => $user->hotelCategoryId,
    //                         //'Status'  => ($user->status == 1) ? 'Active' : 'Inactive',
    //                         'RPK'  => $user->id,
    //                         'AddedBy'     => 1,
    //                         'UpdatedBy'     => 1,
    //                         'created_at'     => now(),
    //                         'updated_at'     => now(),
    //                     ]
    //                 );
    //         }

    //         return [
    //             'status'  => true,
    //             'message' => 'Hotel Master Data synced successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status'  => false,
    //             'message' => $e->getMessage(),
    //         ];
    //     }
    // }



    private function getGstPercent(string $serviceType, float $amount): float
    {
        if ($amount <= 0) {
            return 0;
        }

        static $hasSlabColumns = null;

        // Check columns ONCE
        if ($hasSlabColumns === null) {
            $hasSlabColumns =
                Schema::connection('mysql')->hasColumn('gstmaster', 'priceRangeFrom') &&
                Schema::connection('mysql')->hasColumn('gstmaster', 'priceRangeTo');
        }

        $query = DB::connection('mysql')
            ->table('gstmaster')
            ->where('serviceType', $serviceType)
            ->where('status', 1)
            ->where('deletestatus', 0);

        // Apply slab ONLY if columns exist
        if ($hasSlabColumns) {
            $query->where('priceRangeFrom', '<=', (int) $amount)
                ->where('priceRangeTo', '>=', (int) $amount);
        }

        $row = $query->first();

        return $row ? (float) $row->gstValue : 0;
    }

    ///fast chunk version
    public function hotelMasterSync()
    {
        try {

            /* -------------------------------------------------
         | PRELOAD MASTER TABLES (PERFORMANCE)
         -------------------------------------------------*/
            $destinations = DB::connection('mysql')->table('destinationmaster')->get()->keyBy('name');
            $countries = DB::connection('mysql')->table('countrymaster')->get()->keyBy('name');
            $suppliers = DB::connection('mysql')->table('suppliersmaster')->get()->keyBy('id');
            $roomTypes = DB::connection('mysql')->table('roomtypemaster')->get()->keyBy('id');
            $mealPlans = DB::connection('mysql')->table('mealplanmaster')->get()->keyBy('id');
            $hotelTypes = DB::connection('mysql')->table('hoteltypemaster')->get()->keyBy('id');
            $hotelCats = DB::connection('mysql')->table('hotelcategorymaster')->get()->keyBy('id');
            $marketTypes = DB::connection('mysql')->table('marketmaster')->get()->keyBy('id');
            $terrifTypes = DB::connection('mysql')->table('tarifftypemaster')->get()->keyBy('id');
            $seasonTypes = DB::connection('mysql')->table('seasonmaster')->get()->keyBy('id');
            //$paxTypes     = DB::connection('mysql')->table('paxtypemaster')->get()->keyBy('id');

            $hotels = DB::connection('mysql')->table('packagebuilderhotelmaster')->get();

            foreach ($hotels as $user) {

                /* -------------------------------------------------
             | BASIC IDS
             -------------------------------------------------*/
                $hotelCityId = $destinations[$user->hotelCity]->id ?? null;
                $countryId = $countries[$user->hotelCountry]->id ?? null;
                $uniqueId = 'HOTL' . str_pad($user->id, 6, '0', STR_PAD_LEFT);

                /* -------------------------------------------------
             | HOTEL ROOM TYPES
             -------------------------------------------------*/
                $hotelRoomTypes = [];
                if (!empty($user->roomType)) {
                    $roomTypeIds = array_map('trim', explode(',', $user->roomType));
                    foreach ($roomTypeIds as $rt) {
                        $hotelRoomTypes[] = [
                            "RoomTypeId" => (int) $rt,
                            "RoomTypeName" => $roomTypes[$rt]->name ?? ""
                        ];
                    }
                }

                /* -------------------------------------------------
             | HOTEL AMENITIES
             -------------------------------------------------*/
                $hotelAmenities = !empty($user->amenities)
                    ? array_values(array_filter(array_map('trim', explode(',', $user->amenities))))
                    : [];

                /* -------------------------------------------------
             | HOTEL BASIC DETAILS JSON
             -------------------------------------------------*/
                $hotelBasicDetailsJson = json_encode([
                    "Verified" => (int) ($user->verified ?? 0),
                    "HotelGSTN" => $user->gstn ?? "",
                    "HotelInfo" => $user->hotelInfo ?? "",
                    "HotelLink" => $user->hoteldetail ?? "",
                    "HotelType" => (int) $user->hotelTypeId,
                    "HotelChain" => (int) $user->hotelChain,
                    "CheckInTime" => $user->checkInTime ?? "",
                    "CheckOutTime" => $user->checkOutTime ?? "",
                    "HotelPolicy" => $user->policy ?? "",
                    "HotelAddress" => $user->hotelAddress ?? "",
                    "InternalNote" => $user->internalNote ?? "",
                    "HotelCategory" => (int) $user->hotelCategoryId,
                    "HotelRoomType" => $hotelRoomTypes,
                    "HotelAmenities" => $hotelAmenities
                ], JSON_UNESCAPED_UNICODE);

                /* -------------------------------------------------
             | HOTEL CONTACTS
             -------------------------------------------------*/
                $contacts = DB::connection('mysql')
                    ->table('hotelcontactpersonmaster')
                    ->where('corporateId', $user->id)
                    ->get()
                    ->map(fn($c) => [
                        "Division" => $c->division ?? '',
                        "NameTitle" => $c->nameTitle ?? '',
                        "FirstName" => $c->contactPerson ?? $c->firstName,
                        "LastName" => $c->lastName ?? '',
                        "Designation" => $c->designation ?? '',
                        "CountryCode" => $c->countryCode ?? '',
                        "Phone1" => $c->phone ?? '',
                        "Phone2" => $c->phone2 ?? '',
                        "Phone3" => $c->phone3 ?? '',
                        "Email" => $c->email ?? '',
                        "SecondaryEmail" => $c->email2 ?? '',
                    ]);

                $hotelContactJson = json_encode($contacts, JSON_UNESCAPED_UNICODE);

                /* -------------------------------------------------
             | RATE HEADER (RESTORED)
             -------------------------------------------------*/
                $header = [
                    "RateChangeLog" => [
                        [
                            "ChangeDateTime" => "",
                            "ChangedByID" => "",
                            "ChangeByValue" => "",
                            "ChangeSetDetail" => [
                                ["ChangeFrom" => "", "ChangeTo" => ""]
                            ]
                        ]
                    ]
                ];

                /* -------------------------------------------------
             | HOTEL RATES
             -------------------------------------------------*/
                $rates = DB::connection('mysql')
                    ->table('dmcroomtariff')
                    ->where('serviceid', $user->id)
                    ->get();

                $rateDetailsList = [];

                foreach ($rates as $r) {

                    $uuid = (string) Str::uuid();
                    $paxType = $r->paxType;
                    if ($paxType == 1) {
                        $paxTypeId = 2;
                        $paxTypeName = 'GIT';
                    } else if ($paxType == 2) {
                        $paxTypeId = 1;
                        $paxTypeName = 'FIT';
                    } else {
                        $paxTypeId = 3;
                        $paxTypeName = 'Both';
                    }

                    $seasonName = '';

                    if (!empty($seasonTypes[$r->seasonType]?->seasonNameId)) {
                        $seasonName = match ((int) $seasonTypes[$r->seasonType]->seasonNameId) {
                            1 => 'Summer',
                            2 => 'Winter',
                            3 => 'All',
                            default => ''
                        };
                    }

                    $roomBedTypeArray = [];
                    $roomTotalCost = 0;

                    $roomMap = [
                        'SGL' => (float) $r->singleoccupancy,
                        'DBL' => (float) $r->doubleoccupancy,
                        'TPL' => (float) $r->tripleoccupancy,
                        'ExtraBed(A)' => (float) $r->extraBed,
                        'ExtraBed(C)' => (float) $r->childwithextrabed,
                    ];

                    foreach ($roomMap as $roomName => $roomCost) {

                        $roomCost = (float) $roomCost;

                        $gstPercent = 0;

                        // if ($roomCost > 0) {
                        //     $gstSlab = DB::connection('mysql')
                        //         ->table('gstmaster')
                        //         ->where('serviceType', 'Hotel')
                        //         ->where('status', 1)
                        //         ->where('deletestatus', 0)
                        //         ->where('priceRangeFrom', '<=', (int)$roomCost)
                        //         ->where('priceRangeTo', '>=', (int)$roomCost)
                        //         ->first();

                        //     if ($gstSlab) {
                        //         $gstPercent = (float)$gstSlab->gstValue;
                        //     }
                        // }

                        $gstPercent = $this->getGstPercent('Hotel', $roomCost);

                        $gstAmount = round(($roomCost * $gstPercent) / 100, 2);
                        $totalCost = round($roomCost + $gstAmount, 2);

                        $roomTotalCost += $totalCost;

                        $roomBedTypeArray[] = [
                            "RoomBedTypeName" => $roomName,
                            "RoomCost" => number_format($roomCost, 2, '.', ''),
                            "RoomTaxValue" => $gstPercent . '%',
                            "RoomCostRateValue" => number_format($gstAmount, 2, '.', ''),
                            "RoomTotalCost" => number_format($totalCost, 2, '.', ''),
                        ];
                    }

                    $mealTypeArray = [];
                    $mealTotalCost = 0;

                    $mealMap = [
                        'Breakfast' => (float) $r->breakfast,
                        'Lunch' => (float) $r->lunch,
                        'Dinner' => (float) $r->dinner,
                    ];

                    foreach ($mealMap as $mealName => $mealCost) {

                        $mealCost = (float) $mealCost;
                        $gstPercent = 0;

                        // if ($mealCost > 0) {
                        //     $gstSlab = DB::connection('mysql')
                        //         ->table('gstmaster')
                        //         ->where('serviceType', 'Restaurant')
                        //         ->where('status', 1)
                        //         ->where('deletestatus', 0)
                        //         ->where('priceRangeFrom', '<=', (int)$mealCost)
                        //         ->where('priceRangeTo', '>=', (int)$mealCost)
                        //         ->first();

                        //     if ($gstSlab) {
                        //         $gstPercent = (float)$gstSlab->gstValue;
                        //     }
                        // }
                        $gstPercent = $this->getGstPercent('Restaurant', $mealCost);
                        $gstAmount = round(($mealCost * $gstPercent) / 100, 2);
                        $totalCost = round($mealCost + $gstAmount, 2);

                        $mealTotalCost += $totalCost;

                        $mealTypeArray[] = [
                            "MealTypeName" => $mealName,
                            "MealCost" => number_format($mealCost, 2, '.', ''),
                            "MealTaxValue" => $gstPercent . '%',
                            "MealCostRateValue" => number_format($gstAmount, 2, '.', ''),
                            "MealTotalCost" => number_format($totalCost, 2, '.', ''),
                        ];
                    }


                    $rateDetailsList[] = [
                        "UniqueID" => $uuid,

                        "SupplierId" => $r->supplierId,
                        "SupplierName" => $suppliers[$r->supplierId]->name ?? "",

                        "HotelTypeId" => $user->hotelTypeId,
                        "HotelTypeName" => $hotelTypes[$user->hotelTypeId]->hotelCategory ?? "",

                        "HotelCategoryId" => $user->hotelCategoryId,
                        "HotelCategoryName" => $hotelCats[$user->hotelCategoryId]->name ?? "",

                        "ValidFrom" => $r->fromDate,
                        "ValidTo" => $r->toDate,

                        "MarketTypeId" => (int) $r->marketType,
                        "MarketTypeName" => $marketTypes[$r->marketType]->name ?? "",

                        "PaxTypeId" => $paxTypeId,
                        "PaxTypeName" => $paxTypeName,

                        "TarrifeTypeId" => (int) $r->tarifType,
                        "TarrifeTypeName" => $terrifTypes[$r->tarifType]->name ?? "",
                        "SeasonTypeID" => (int) $r->seasonType,
                        "SeasonTypeName" => $seasonName ?? "",
                        "SeasonYear" => $r->seasonYear,

                        "RoomTypeId" => (int) $r->roomType,
                        "RoomTypeName" => $roomTypes[$r->roomType]->name ?? "",

                        "MealPlanId" => $r->mealPlan,
                        "MealPlanName" => $mealPlans[$r->mealPlan]->name ?? "",

                        "CurrencyId" => (int) $r->currencyId,
                        "CurrencyName" => "INR",

                        "RoomBedType" => $roomBedTypeArray,

                        "MealType" => $mealTypeArray,

                        "TAC" => $r->roomTAC,
                        "MarkupType" => $r->markupType,
                        "MarkupCost" => $r->markupCost ?? 0,

                        "TotalCost" => number_format(
                            $r->roomprice + ($r->breakfast + $r->lunch + $r->dinner),
                            2,
                            '.',
                            ''
                        ),

                        "Status" => "Active",
                        "BlackoutDates" => [],
                        "GalaDinner" => [],
                    ];
                }

                /* -------------------------------------------------
             | RATE JSON
             -------------------------------------------------*/
                $rateJson = !empty($rateDetailsList)
                    ? json_encode([
                        "HotelId" => $user->id,
                        "HotelUUID" => $uniqueId,
                        "HotelName" => $user->hotelName,
                        "DestinationID" => $hotelCityId,
                        "Header" => $header,
                        "Data" => [
                            [
                                "Total" => count($rateDetailsList),
                                "RateDetails" => $rateDetailsList
                            ]
                        ]
                    ], JSON_UNESCAPED_UNICODE)
                    : null;

                /* -------------------------------------------------
             | HOTEL SEARCH (CHUNK INSERT)
             -------------------------------------------------*/
                $searchBatch = [];

                foreach ($rateDetailsList as $rate) {
                    $start = Carbon::parse($rate['ValidFrom']);
                    $end = Carbon::parse($rate['ValidTo']);

                    while ($start->lte($end)) {
                        $searchBatch[] = [
                            "ServiceRateUniqueId" => $rate['UniqueID'],
                            "HotelID" => $uniqueId,
                            "date" => $start->format('Y-m-d'),
                            "DestinationID" => 'DES' . str_pad($hotelCityId, 6, '0', STR_PAD_LEFT),
                            "SupplierID" => 'SUPP' . str_pad($rate['SupplierId'], 6, '0', STR_PAD_LEFT),
                            "CurrencyID" => $rate['CurrencyId'],
                            "RateJson" => json_encode($rate, JSON_UNESCAPED_UNICODE),
                            "Status" => "Active",
                            "created_at" => now(),
                            "updated_at" => now()
                        ];
                        $start->addDay();
                    }
                }

                foreach (array_chunk($searchBatch, 500) as $chunk) {
                    DB::connection('pgsql')->table('hotel.hotel_search')->insert($chunk);
                }

                /* -------------------------------------------------
             | HOTEL MASTER
             -------------------------------------------------*/
                DB::connection('pgsql')
                    ->table('hotel.hotel_master')
                    ->updateOrInsert(
                        ['id' => $user->id],
                        [
                            'HotelName' => $user->hotelName,
                            'HotelCountry' => $countryId,
                            'HotelCity' => $hotelCityId,
                            'HotelBasicDetails' => $hotelBasicDetailsJson,
                            'HotelContactDetails' => $hotelContactJson,
                            'RateJson' => $rateJson,
                            'UniqueID' => $uniqueId,
                            'Destination' => $hotelCityId,
                            'default' => 'No',
                            'SupplierId' => $user->supplierId,
                            'HotelTypeId' => $user->hotelTypeId,
                            'HotelCategory' => $user->hotelCategoryId,
                            'HotelAddress' => $user->hotelAddress,
                            'created_at' => now(),
                            'updated_at' => now()
                        ]
                    );
            }

            return ['status' => true, 'message' => 'Hotel Master synced successfully'];
        } catch (\Throwable $e) {
            return ['status' => false, 'message' => $e->getMessage()];
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'Status' => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Room Type Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'CountryId' => $user->country,
                            'Name' => $user->name,
                            'CountryCode' => $user->currencyCode,
                            'ConversionRate' => $user->currencyValue,
                            'SetDefault' => $user->setDefault,
                            'Status' => "Active",
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Currency Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
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
                            'id' => $user->id,
                            'Name' => $user->name,
                            'SetDefault' => $user->setDefault,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Business Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function seasonMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('seasonmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                // ✅ Direct season name condition
                if (!empty($user->name)) {
                    $seasonName = $user->name;
                } else {
                    $seasonName = match ((int) ($user->seasonNameId ?? 0)) {
                        1 => 'Summer',
                        2 => 'Winter',
                        3 => 'All',
                        default => ''
                    };
                }

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.season_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $seasonName,
                            'SeasonName' => $user->name ?? "",
                            'FromDate' => $user->fromDate,
                            'ToDate' => $user->toDate,
                            'Default' => 0,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Season Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function hsnSacMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('saccodemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.sac_code_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'ServiceType' => $user->serviceType,
                            'SacCode' => $user->sacCode,
                            'SetDefault' => $user->setDefault,
                            'GstSlabId' => $user->taxSlab,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'HSN/SAC Code Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function gstMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('gstmaster')
                ->get();

            foreach ($mysqlUsers as $user) {
                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.tax_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'ServiceType' => $user->serviceType,
                            'TaxSlabName' => $user->gstSlabName,
                            'TaxValue' => $user->gstValue,
                            'SetDefault' => $user->setDefault,
                            'PriceRangeFrom' => $user->priceRangeFrom ?? '',
                            'PriceRangeTo' => $user->priceRangeTo ?? '',
                            // 'Currency' => $user->currencyId ?? '',
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'GST Tax Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function companyAddressMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('officebranches')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('administrator.company_offcename')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'CompanyId' => 1,
                            'OfficeName' => $user->name,
                            'Country' => $user->countryId,
                            'State' => $user->stateId,
                            'City' => $user->cityId,
                            'Address' => $user->address . " Pin-" . $user->pinCode,
                            'ContacctPersonName' => "",
                            'Email' => $user->email ?? '',
                            'Phone' => $user->contactNumber ?? '',
                            'Mobile' => $user->contactNumber ?? '',
                            'GstNo' => $user->gstn,
                            'Currency' => 0,
                            'office_type' => $user->addressType,
                            'Pan' => $user->PAN ?? '',
                            'Cin' => $user->CIN ?? '',
                            'Iec' => $user->IEC ?? '',
                            'Website' => $user->web_url ?? '',
                            'CountryCode' => $user->countryCode ?? '',
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Company Address Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    private function buildQueryJson($user, $queryId)
    {
        // 1) Raw string from DB (e.g. "1,7,7,9,9,3,3,3,2,2,2,1,")
        $raw = trim($user->destinationId ?? '');

        // 2) Remove trailing comma
        $raw = rtrim($raw, ',');

        // 3) Convert to array
        $destinationIds = explode(',', $raw);

        $travelData = [];
        $startDate = $this->fixDate($user->fromDate);
        $endDate = $this->fixDate($user->toDate);
        $total = count($destinationIds);  // total items
        $dayNo = 1;
        // Convert to DateTime for incrementing
        $dateObj = new \DateTime($startDate);

        foreach ($destinationIds as $index => $dest) {
            $destinationName = null;
            // FIRST item → flight
            if ($index == 0) {
                $mode = "flight";

                // LAST item → flight
            } elseif ($index == $total - 1) {
                $mode = "flight";

                // MIDDLE items → surface
            } else {
                $mode = "surface";
            }

            if ($dest) {
                $destination = DB::connection('mysql')
                    ->table('destinationmaster')
                    ->where('id', $dest)
                    ->first();

                $destinationName = $destination->name ?? null;
            }

            $travelData[] = [
                "Date" => $dateObj->format('Y-m-d'),
                "DayNo" => $dayNo,
                "Destination" => $dest,
                "Enroute" => null,
                "Mode" => $mode, // default
                "isEnroute" => false,
                "DestinationName" => $destinationName,        // you can map later
                "EnrouteName" => ""
            ];

            // increment date by 1 day
            $dateObj->modify('+1 day');
            $dayNo++;
        }

        $contactName = null;
        $contactEmail = null;
        $contactPhone = null;
        $contactAddress = null;

        if ($user->clientType == 1) {
            if ($user->companyId) {
                $corporatedetails = DB::connection('mysql')
                    ->table('corporatemaster')
                    ->where('id', $user->companyId)
                    ->first();

                $contactName = $corporatedetails?->name;
                $contactEmail = $this->safeTripleDecode($corporatedetails?->companyEmail ?? '');
                $contactPhone = $this->safeTripleDecode($corporatedetails?->companyPhone ?? '');
                $contactAddress = $corporatedetails?->address1;
            }
        }

        if ($user->clientType == 2) {
            if ($user->companyId) {
                $corporatedetails = DB::connection('mysql')
                    ->table('contactsmaster')
                    ->where('id', $user->companyId)
                    ->first();

                $contactName = $corporatedetails?->firstName . " " . $corporatedetails?->lastName;
                $contactAddress = $corporatedetails?->address1;
            }
        }

        $paxType = $user->paxType;
        if ($paxType == 1) {
            $paxTypeId = 2;
            $paxTypeName = 'GIT';
        } else if ($paxType == 2) {
            $paxTypeId = 1;
            $paxTypeName = 'FIT';
        } else {
            $paxTypeId = 3;
            $paxTypeName = 'Both';
        }


        return [
            "QueryID" => $queryId,
            "CompanyId" => 1 ?? '',
            "ClientName" => $user->leadPaxName ?? '',
            "CompanyName" => $user->companyName ?? "",
            "UserId" => $user->addedBy ?? '',
            "UserName" => $user->userName ?? '',
            "UserType" => $user->userType ?? [],
            "Budget" => $user->budget ?? '',
            "Header" => [
                "QueryStatus" => $user->queryStatus ?? '',
                "QueryChangeLog" => [
                    [
                        "ChangeDateTime" => $user->changeDateTime ?? '',
                        "ChangedByID" => $user->changedById ?? '',
                        "ChangeByValue" => $user->changeByValue ?? '',
                        "ChangeSetDetail" => [
                            [
                                "ChangeFrom" => $user->changeFrom ?? '',
                                "ChangeTo" => $user->changeTo ?? '',
                            ]
                        ],
                    ]
                ],
            ],
            "MealPlan" => $user->mealPlan ?? '',
            "MealPlanName" => $user->mealPlanName ?? '',
            "Consortia" => $user->consortia ?? '',
            "ConsortiaName" => $user->consortiaName ?? '',
            "Language" => $user->language ?? '',
            "LanguageName" => $user->languageName ?? '',
            "ISO" => $user->iso ?? '',
            "ISOName" => $user->isoName ?? '',

            // Example QueryType (array)
            "QueryType" => [
                [
                    "QueryTypeId" => $user->queryType,
                    "QueryTypeName" => ($user->queryType == 1) ? 'Query' : ''
                ]
            ],
            "PaxInfo" => [
                "PaxType" => $paxTypeId,
                "PaxTypeName" => $paxTypeName,
                "TotalPax" => $user->adult + $user->child,
                "Adult" => $user->adult,
                "Child" => $user->child,
                "Infant" => $user->infant
            ],
            "ContactInfo" => [
                "ContactId" => $user->companyId,
                "ContactPersonName" => $contactName,
                "ContactNumber" => $contactPhone,
                "ContactEmail" => $contactEmail,
                "ContactAddress" => $contactAddress
            ],
            "TravelDateInfo" => [
                "ScheduleType" => "Date Wise",
                "SeasonType" => $user->seasonType,
                "SeasonTypeName" => '',
                "SeasonYear" => $user->seasonYear,
                "TotalNights" => $total > 0 ? $total - 1 : 0,
                "TotalNoOfDays" => $total,
                "FromDate" => $startDate,
                "FromDateDateWise" => "",
                "ToDateDateWise" => null,
                "ToDate" => $endDate,
                "TravelData" => $travelData,
                "ArrivalDate" => $startDate,
                "DepartureDate" => $endDate
            ],

            // Preferences
            "Prefrences" => json_decode($user->prefrencesJson ?? '{}', true),

            // Description
            "Description" => $user->description ?? '',

            "TravelType" => $user->travelType ?? '',

            "CurrencyId" => $user->currencyId ?? '',
            "CurrencyName" => $user->currencyName ?? '',
            "ConversionRate" => $user->conversionRate ?? '',

            // Hotels (rooms)
            "Hotel" => json_decode($user->hotelJson ?? '{}', true),

            // Additional Services
            "ValueAddedServiceDetails" => json_decode($user->valueAddedJson ?? '{}', true),
        ];
    }

    private function buildQuotationJson($user, $queryId)
    {
        // ------------------------
        // 1) Build Tour Summary → TravelData (same logic as Query JSON)
        // ------------------------
        $raw = trim($user->destinationId ?? '');
        $raw = rtrim($raw, ',');
        $destinationIds = explode(',', $raw);

        $travelData = [];
        $startDate = $this->fixDate($user->fromDate);
        $endDate = $this->fixDate($user->toDate);

        $total = count($destinationIds);
        $dayNo = 1;
        $dateObj = new \DateTime($startDate);

        foreach ($destinationIds as $index => $dest) {
            $destinationName = null;

            if ($index == 0 || $index == $total - 1) {
                $mode = "flight";
            } else {
                $mode = "surface";
            }

            if ($dest) {
                $destination = DB::connection('mysql')
                    ->table('destinationmaster')
                    ->where('id', $dest)
                    ->first();
                $destinationName = $destination->name ?? null;
            }

            $travelData[] = [
                "Date" => $dateObj->format('Y-m-d'),
                "DayNo" => $dayNo,
                "Destination" => $dest,
                "Enroute" => null,
                "Mode" => $mode,
                "isEnroute" => false,
                "DestinationName" => $destinationName,
                "EnrouteName" => ""
            ];

            $dateObj->modify('+1 day');
            $dayNo++;
        }

        // ------------------------
        // 2) Prepare Day Wise Details (Days[])
        // ------------------------
        $days = [];
        $dayNo = 1;
        $dateObj = new \DateTime($startDate);

        foreach ($destinationIds as $dest) {
            $destination = DB::connection('mysql')
                ->table('destinationmaster')
                ->where('id', $dest)
                ->first();

            $days[] = [
                "Day" => $dayNo,
                "DayUniqueId" => Str::uuid()->toString(),
                "Date" => $dateObj->format('Y-m-d'),
                "DestinationId" => $dest,
                "DestinationUniqueId" => "DEST" . str_pad($dest, 5, "0", STR_PAD_LEFT),
                "DestinationName" => $destination->name ?? "",
                "EnrouteId" => null,
                "EnrouteName" => "",
                "DayTotal" => null,
                "DayTaxValue" => null,
                "DayCurrencyType" => null,
                "OptinalExperience" => [
                    "OptionalServiceDetails" => []
                ],
                "DayServices" => []
            ];

            $dayNo++;
            $dateObj->modify("+1 day");
        }

        // ------------------------
        // 3) Final JSON Return (Matches your provided JSON exactly)
        // ------------------------
        return [
            "QuotationNumber" => $queryId . '-A' ?? "",
            "TourId" => "",
            "ReferenceId" => "",
            "Header" => [
                "QuotationStage" => "",
                "QuotationStatus" => "4",
                "QuotationVersion" => "A",
                "PaxSlabType" => $user->paxSlabType ?? "Single Slab",
                "Subject" => $user->subject ?? "",
                "HotelMarkupType" => $user->hotelMarkupType ?? "Service Wise Markup",
                "PackageId" => null,
                "HotelCategory" => "Single Hotel Category",
                "HotelStarCategories" => [],
                "QuotationChangeLog" => [
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
            ],

            // ---------------- TOUR SUMMARY ----------------
            "TourSummary" => [
                "TourDetails" => $travelData,
                "FromDate" => $startDate,
                "ToDate" => $endDate,
                "NumberOfDays" => count($destinationIds),
                "NumberOfNights" => (count($destinationIds) - 1),
                "PaxTypeId" => $user->paxTypeId ?? "1",
                "PaxTypeName" => $user->paxTypeName ?? "FIT",
                "PaxCount" => $user->paxCount ?? 2,
                "Destination" => implode("^", array_map(function ($i) use ($destinationIds) {
                    return ($i + 1) . "~" . $destinationIds[$i];
                }, array_keys($destinationIds))),
                "TourType" => "",
                "TourServiceSummary" => ""
            ],

            // ---------------- PAX ----------------
            "Pax" => [
                "AdultCount" => $user->adultCount ?? 2,
                "ChildCount" => $user->childCount ?? 0,
                "Child" => []
            ],

            // ---------------- COST ----------------
            "CustomerCost" => [
                "CustomerCostDetails" => [
                    "QuotationCost" => null,
                    "Paidvalue" => null
                ],
                "CustomerPaymentDetails" => []
            ],

            // ---------------- OVERVIEW & INCLUDE/EXCLUDE ----------------
            "OverviewIncExcTc" => [
                "OverviewId" => "",
                "OverviewName" => "",
                "LanguageId" => "",
                "LanguageName" => "",
                "Overview" => "",
                "ItineraryIntroduction" => "",
                "ItinerarySummary" => ""
            ],
            "FitIncExc" => [
                "FitId" => "",
                "FitName" => "",
                "LanguageId" => "",
                "LanguageName" => "",
                "Inclusion" => "",
                "Exclusion" => "",
                "TermsNCondition" => "",
                "CancellationPolicy" => "",
                "Payment Policy" => "",
                "BookingPolicy" => "",
                "Remarks" => ""
            ],

            // ---------------- QUERY INFO ----------------
            "QueryInfo" => [
                "ContactInfo" => [
                    "ContactId" => $user->contactId ?? "",
                    "ContactPersonName" => "",
                    "ContactNumber" => "",
                    "ContactEmail" => "",
                ],
                "Accomondation" => json_decode($user->accommodationJson ?? '{}', true)
            ],

            // ---------------- MARKUP ----------------
            "Markup" => [
                "MarkupType" => "Service Wise",
                "Data" => [
                    ["Type" => "Universal", "Markup" => "", "Value" => ""],
                    ["Type" => "Hotel", "Markup" => "", "Value" => ""],
                    ["Type" => "Guide", "Markup" => "", "Value" => ""],
                    ["Type" => "Activity", "Markup" => "", "Value" => ""],
                    ["Type" => "Entrance", "Markup" => "", "Value" => ""],
                    ["Type" => "Transfer", "Markup" => "", "Value" => ""],
                    ["Type" => "Enroute", "Markup" => "", "Value" => ""],
                    ["Type" => "Train", "Markup" => "", "Value" => ""],
                    ["Type" => "Flight", "Markup" => "", "Value" => ""],
                    ["Type" => "Restaurant", "Markup" => "", "Value" => ""],
                    ["Type" => "Visa", "Markup" => "", "Value" => ""],
                    ["Type" => "Insurance", "Markup" => "", "Value" => ""],
                    ["Type" => "Others", "Markup" => "", "Value" => ""]
                ]
            ],

            // ---------------- OTHERS ----------------
            "Commision" => [
                "ClientCommision" => ""
            ],
            "SupplimentSelection" => [
                "FlightCost" => "",
                "TourEscort" => ""
            ],
            "MealSuppliment" => [
                "FlightCost" => "",
                "TourEscort" => ""
            ],
            "OthersInfo" => [
                "GstType" => "",
                "Gst" => "",
                "TCS" => "",
                "DiscountType" => "",
                "Discount" => "",
                "SrsandTrr" => "",
                "TermsNCondition" => "",
                "CurrencyId" => "",
                "CurrencyName" => "",
                "ROE" => ""
            ],

            // ---------------- DAY WISE DATA ----------------
            "Days" => $days
        ];
    }

    public function queryMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('querymaster')
                ->get();

            foreach ($mysqlUsers as $user) {
                $displayId = $user->displayId;
                $prefix = 'BS';
                // 2) Generate financial year string, e.g. 2025-2026 → "25-26"
                $currentYear = (int) date('Y');     // e.g. 2025
                $nextYear = $currentYear + 1;     // 2026

                $fyPart = substr($currentYear, -2) . '-' . substr($nextYear, -2); // "25-26"

                // 3) Sequence padded to 6 digits from id
                $seq = str_pad($displayId, 6, '0', STR_PAD_LEFT); // 43 → "000043"

                // 4) Final format: BS25-26/000043
                $queryId = $prefix . $fyPart . '/' . $seq;

                if ($user->clientType == 1) {
                    $clientType = 14;
                }
                if ($user->clientType == 2) {
                    $clientType = 15;
                }

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('querybuilder.query_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'QueryId' => $queryId,
                            'ClientType' => $$clientType ?? 14,
                            'LeadPax' => $user->leadPaxName,
                            'Subject' => $user->subject,
                            'FromDate' => $this->fixDate($user->fromDate),
                            'TAT' => $user->tat,
                            'LeadSource' => $user->leadsource,
                            'ToDate' => $this->fixDate($user->toDate),
                            'Priority' => $user->queryPriority == 1 ? 'Low' : ($user->queryPriority == 2 ? 'Medium' : ($user->queryPriority == 3 ? 'High' : 'Low')),
                            'TourId' => $user->tourId,
                            'ReferenceId' => 0,
                            'QueryStatus' => $user->queryStatus,
                            'CompanyId' => 1,
                            'Fk_QueryId' => 0,
                            'Type' => $user->travelType,
                            'QueryJson' => json_encode($this->buildQueryJson($user, $queryId)),
                            'QuotationJson' => json_encode($this->buildQuotationJson($user, $queryId)),
                            'FinalQuotationAfterOperation' => json_encode($this->buildQuotationJson($user, $queryId)),
                            //'Status'  => $user->status,
                            'RPK' => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Query Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    private function fixDate($date)
    {
        return ($date == "0000-00-00" || $date == null) ? null : $date;
    }

    public function guideMasterSync()
    {
        try {

            $guides = DB::connection('mysql')
                ->table('tbl_guidesubcatmaster')
                ->get();

            foreach ($guides as $g) {

                // 🔹 Destination
                $destination = DB::connection('mysql')
                    ->table('destinationmaster')
                    ->where('id', $g->destinationId ?? null)
                    ->first();

                $destinationJson = json_encode([
                    'id' => $destination->id ?? null,
                    'Name' => $destination->name ?? ''
                ], JSON_UNESCAPED_UNICODE);

                // 🔹 Service Name (safe)
                $serviceName =
                    $g->guideName
                    ?? $g->guidename
                    ?? $g->subcatname
                    ?? $g->name
                    ?? 'Guide Service';

                // 🔹 FORCE max 50 chars (DB-safe)
                $serviceName = Str::limit(trim($serviceName), 50, '');

                // 🔹 Unique ID (PRIMARY KEY replacement)
                $uniqueId = 'GUIS' . str_pad($g->id, 4, '0', STR_PAD_LEFT);

                // 🔹 Insert / Update
                DB::connection('pgsql')
                    ->table('guide.guide_service_master')
                    ->updateOrInsert(
                        ['UniqueID' => $uniqueId],   // ✅ CORRECT KEY
                        [
                            'ServiceType' => 'Guide',
                            'Destination' => $g->destinationId,
                            'Guide_Porter_Service' => $serviceName,
                            'RateJson' => null,
                            'CompanyId' => '',
                            'Default' => 'No',
                            'Status' => 1,
                            'AddedBy' => 1,
                            'UpdatedBy' => 0,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Guide Service Master synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage()
            ];
        }
    }



    public function invoiceMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('invoicemaster')
                ->get();

            foreach ($mysqlUsers as $user) {
                //$displayId = $user->queryId;
                //////////////////////
                $displayId = "";
                $querydata = DB::connection('mysql')
                    ->table('querymaster')
                    ->where('id', $user->queryId)
                    ->first();
                $displayId = $querydata->displayId ?? "";

                $prefix = 'BS';
                // 2) Generate financial year string, e.g. 2025-2026 → "25-26"
                $currentYear = (int) date('Y');     // e.g. 2025
                $nextYear = $currentYear + 1;     // 2026

                $fyPart = substr($currentYear, -2) . '-' . substr($nextYear, -2); // "25-26"

                // 3) Sequence padded to 6 digits from id
                $seq = str_pad($displayId, 6, '0', STR_PAD_LEFT); // 43 → "000043"

                // 4) Final format: BS25-26/000043
                $queryId = $prefix . $fyPart . '/' . $seq;

                //////////////////////
                $currencyName = "";
                $currency = DB::connection('mysql')
                    ->table('querycurrencymaster')
                    ->where('id', $user->currencyId)
                    ->first();
                $currencyName = $currency->name ?? "";
                /////////////////////

                //////////////////////
                $agentCountryId = "";
                // 1️⃣ Try corporatemaster first
                $agentCountryId = DB::connection('mysql')
                    ->table('corporatemaster')
                    ->whereRaw('TRIM(name) = ?', [trim($user->agentName)])
                    ->value('countryId');

                // 2️⃣ Fallback to contactsmaster
                if (!$agentCountryId) {
                    $agentCountryId = DB::connection('mysql')
                        ->table('contactsmaster')
                        ->whereRaw(
                            "TRIM(CONCAT(firstName, ' ', lastName)) = ?",
                            [trim($user->agentName)]
                        )
                        ->value('countryId') ?? "";
                }
                /////////////////////

                //////////////////////
                $CountryName = "";
                $countryData = DB::connection('mysql')
                    ->table('countrymaster')
                    ->where('id', $agentCountryId)
                    ->first();
                $CountryName = $countryData->name ?? "";
                /////////////////////

                //////////////////////
                $bankName = "";
                $bankdetail = DB::connection('mysql')
                    ->table('bankmaster')
                    ->where('id', $user->bankNameItem)
                    ->first();
                $bankName = $bankdetail->bankName ?? "";
                /////////////////////

                //////////////////////
                $deliveryName = "";
                $deliverydetail = DB::connection('mysql')
                    ->table('statemaster')
                    ->where('id', $user->deliveryPlace)
                    ->first();
                $deliveryName = $deliverydetail->name ?? "";
                /////////////////////

                ////////////////////
                $seqSetting = DB::connection('mysql')
                    ->table('invoicesequencesetting')
                    ->where('officeId', $user->officeCode)
                    ->first();

                $baseFormat = "";
                $lastNumber = 0;

                // 🔵 DETERMINE TAX OR PROFORMA
                if ($user->officewise_proformasq > 0) {
                    // PROFORMA
                    $lastNumber = $user->officewise_proformasq;
                    $settingFormat = $seqSetting->profromaInvoiceSequence ?? 'PI-';
                } else if ($user->officewise_taxsq > 0) {
                    // TAX
                    $lastNumber = $user->officewise_taxsq;
                    $settingFormat = $seqSetting->taxInvoiceSequence ?? 'TAX-';
                }

                // Remove extra numeric suffix (like 01) from the end
                $settingFormat = preg_replace('/\d+$/', '', $settingFormat);
                // 🔵 CREATE FINAL INVOICE NUMBER
                $nextNumber = str_pad($lastNumber, 4, '0', STR_PAD_LEFT);
                $invoiceNumber = $settingFormat . $nextNumber;
                ///////////////////

                ////////////////////
                DB::connection('pgsql')
                    ->table('querybuilder.query_master')
                    ->where('id', $user->queryId)
                    ->update([
                        'FileNo' => $user->fileNo ?? '',
                        'TourId' => $user->tourId ?? '',
                    ]);
                ////////////////////

                $total = $user->totalTourCost;
                $total = is_numeric($total) ? (float) $total : 0;
                $total = round($total, 2);

                $particularRows = DB::connection('mysql')
                    ->table('multipleinvoicemaster')
                    ->where('invoiceId', $user->id)
                    ->get();
                $particulars = [];

                foreach ($particularRows as $row) {
                    $amount = is_numeric($row->amount) ? (float) $row->amount : 0;
                    $totalamount = is_numeric($row->totalamount) ? (float) $row->totalamount : 0;
                    $totalTourCost = is_numeric($row->totalTourCost) ? (float) $row->totalTourCost : 0;
                    $totalCostWithoutGST = is_numeric($row->totalCostWithoutGST) ? (float) $row->totalCostWithoutGST : 0;

                    $taxVlaue = $row->gstTax / 2;

                    $particulars[] = [
                        "description" => $row->particularsubject ?? '',
                        "ParticularName" => $row->particularsubject ?? '',
                        "Pax" => $row->totalPax ?? '',
                        "HSN" => $row->hsnCodeId ?? '',
                        "SAC" => $row->hsnCodeId ?? '',
                        "Amount" => number_format($amount, 2),
                        "Tcs" => "%",
                        "Tax" => "%",
                        "TotalAmount" => number_format($totalamount, 2),
                        "GSTId" => ($row->igst != '') ? $row->gstTax : 0,
                        "StateChange" => $row->gstType == 1 ? "Same State" : ($row->gstType == 2 ? "Other State" : ""),
                        "Igst" => ($row->igst != '') ? $row->gstTax : 0,
                        "IgstAmount" => number_format(is_numeric($row->igst) ? $row->igst : 0, 2),
                        "CgstAmount" => number_format(is_numeric($row->cgst) ? $row->cgst : 0, 2),
                        "SgstAmount" => number_format(is_numeric($row->SGST) ? $row->SGST : 0, 2),
                        "Cgst" => $taxVlaue ?? 0,
                        "Sgst" => $taxVlaue ?? 0,
                        "ExcludeGstorNot" => ($row->isTaxableVal == 1) ? 'Yes' : 'No',
                        "TotalTourCost" => number_format($totalamount, 2),
                        "IsTaxable" => ($row->isTaxableVal == 1) ? 'Yes' : 'No',
                        "TaxType" => ($row->isExclusiveTax == 2) ? 'Inclusive' : 'Exclusive',
                        "ppCost" => number_format($amount, 2),
                        "TaxableValue" => number_format($totalCostWithoutGST, 2),
                    ];
                }
                // -------------------------------
                // ✅ Fix InvoiceDetails JSON
                // -------------------------------
                $invoiceDetails = [
                    "OfficeId" => $user->officeCode ?? '',
                    "FormatType" => $user->invoiceFormat == 11 ? "ItemWise" : ($user->invoiceFormat == 1 ? "FileWise" : ""),
                    "TourRefNo" => $user->tourId ?? '',
                    "DisplayTaxRate" => "yes",
                    "DisplayGstNo" => "",
                    "DisplayCinNo" => "",
                    "DisplayPlaceOfSupply" => "yes",
                    "DisplayAgent" => "",
                    "DisplaySacCode" => "",
                    "DisplayArnNo" => "",
                    "CostType" => "Individual",
                    "GstType" => "",
                    "ClientName" => $user->agentName ?? '',
                    "Tcs" => $user->tcs ?? '',
                    "TourAmount" => $total ?? '',
                    "CompanyLogo" => "",
                    "CompanyName" => $user->beneficiaryName ?? '',
                    "CompanyAddress" => "",
                    "CompanyGst" => "",
                    "CompanyCity" => "",
                    "CompanyState" => "",
                    "CompanyContact" => "",
                    "CompanyEmail" => "",
                    "CompanyWebsite" => "",
                    "CompanyPan" => "",
                    "CompanyCIN" => "",
                    "BillToCompanyName" => $user->agentName ?? '',
                    "BillToCompanyAddress" => $user->clientAddress ?? '',
                    "BillToCompanyContact" => $user->clientPhone ?? '',
                    "BillToCompanyEmail" => $user->clientEmail ?? '',
                    "BillToCompanyWebsite" => "",
                    "BillToCompanyPan" => $user->panInformation ?? '',
                    "BillToCompanyCIN" => "",
                    "BillToCountry" => $CountryName ?? '',
                    "InvoiceNo" => $invoiceNumber ?? '',
                    "InvoiceDate" => $this->fixDate($user->invoicedate ?? null),
                    "ReferenceNo" => $user->refNo ?? '',
                    "DueDate" => $this->fixDate($user->dueDate ?? null),
                    "ToutDate" => "",
                    "FileNo" => $user->FileNo ?? '',
                    "Currency" => $user->currencyId ?? '',
                    "GuestNameorReceiptName" => $user->guestName ?? '',
                    "PlaceofDeliveryId" => $user->deliveryPlace,
                    "PlaceofDeliveryName" => $deliveryName,
                    "ROE" => $user->dayRoe,
                    "Particulars" => $particulars,
                    "TotalTourCost" => $total,
                    "Cgst" => $user->cgst ?? '',
                    "Sgst" => $user->gst ?? '',
                    'GrantTotal' => $total,
                    'GrantTotalInWords' => '',
                    'CgstPercent' => '',
                    'SgstPercent' => '',
                    'InvoiceType' => $user->invoiceFormat == 11 ? "ItemWise" : ($user->invoiceFormat == 1 ? "FileWise" : ""),
                    'StateCode' => '',
                    'CgstDetail' => '',
                    'CurrencyName' => $currencyName,
                    'Category' => 'TOUR OPERATOR',
                    'CreatedBy' => "",
                    'showtaxvalue' => '',
                    "BankDetails" => [
                        [
                            "BankName" => $bankName ?? '',
                            "AmountType" => $bankdetail->accountType ?? "",
                            "baneficiaryName" => $bankdetail->beneficiaryName ?? '',
                            "AccountNumber" => $bankdetail->accountNumber ?? '',
                            "IFSC" => $bankdetail->branchIFSC ?? '',
                            "BranchAddress" => $bankdetail->branchAddress ?? '',
                            "BranchSwiftCode" => $bankdetail->branchSwiftCode ?? '',
                        ]
                    ],

                    "TermsandCondition" => "",
                    "PaymentDesc" => "",
                    "StateChange" => $user->gstType == 1 ? "Same State" : ($user->gstType == 2 ? "Other State" : ""),
                ];

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('querybuilder.invoice')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'InvoiceId' => $invoiceNumber,
                            'Type' => $user->invoiceTitle == 1 ? 'Tax' : ($user->invoiceTitle == 2 ? 'PI' : ''),
                            'QueryId' => $queryId,
                            'QuotationNo' => $queryId . "-A Final",
                            'TourId' => $user->tourId ?? '',
                            'ReferenceId' => '',
                            'InvoiceDetails' => json_encode($invoiceDetails, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                            'PdfFileLink' => '',
                            'CompanyId' => 1,
                            'OperationId' => 0,
                            'DepartmentId' => 0,
                            'FinalPayment' => $total,
                            'InvoiceType' => $user->invoiceFormat == 11 ? 'ItemWise' : ($user->invoiceFormat == 1 ? 'FileWise' : ''),
                            'Html' => "",
                            //'Status'  => $user->status,
                            'RPK' => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Invoice Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function agentContactSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('contactpersonmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.contact_person_master')
                    ->updateOrInsert(
                        //['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'ParentId' => $user->corporateId,
                            'OfficeName' => "Head Office",
                            //'MetDuring'           => "",
                            'Title' => "",
                            'FirstName' => $user->contactPerson ?? '',
                            'LastName' => $user->lastName ?? '',
                            'Email' => $this->safeTripleDecode($user->email) ?? '',
                            'Phone' => $this->safeTripleDecode($user->phone) ?? '',
                            'MobileNo' => $this->safeTripleDecode($user->phone) ?? '',
                            'Designation' => $user->designation ?? '',
                            'Division' => isset($user->division) && is_numeric($user->division)
                                ? (int) $user->division
                                : null,
                            'CountryCode' => $user->countryCode,
                            'type' => 'Agent',
                            'Status' => 'Yes',
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Agent Contact Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    ////////////////test////////////
    public function dmcSync()
    {
        try {
            $mysqlUsers = DB::connection('mysql')
                ->table('packagebuilderentrancemaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                //------------------------------------
                // DESTINATION
                //------------------------------------
                $destinationId = null;
                $destinationName = "";

                if ($user->entranceCity) {
                    $destination = DB::connection('mysql')
                        ->table('destinationmaster')
                        ->where('name', $user->entranceCity)
                        ->first();

                    $destinationId = $destination->id ?? null;
                    $destinationName = $destination->name ?? "";
                }

                //------------------------------------
                // CLOSE DAYS JSON
                //------------------------------------
                $closeDaysnameJson = !empty($user->closeDaysname)
                    ? json_encode(array_values(array_filter(
                        array_map('trim', explode(',', $user->closeDaysname)),
                        fn($v) => $v !== ""
                    )))
                    : json_encode([]);

                //------------------------------------
                // UNIQUE ID
                //------------------------------------
                $uniqueId = !empty($user->id)
                    ? 'SIGH' . str_pad($user->id, 6, '0', STR_PAD_LEFT)
                    : '';

                //------------------------------------
                // FETCH RATES
                //------------------------------------
                $rates = DB::connection('mysql')
                    ->table('dmcentrancerate')
                    ->where('entranceNameId', $user->id)
                    ->get();

                //------------------------------------
                // HEADER
                //------------------------------------
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

                //------------------------------------
                // BUILD RATE DETAILS (IF ANY)
                //------------------------------------
                $rateDetails = [];

                foreach ($rates as $r) {

                    // Supplier Name
                    $supplierName = "";
                    if (!empty($r->supplierId)) {
                        $sup = DB::connection('mysql')
                            ->table('suppliersmaster')
                            ->where('id', $r->supplierId)
                            ->first();

                        $supplierName = $sup->name ?? "";
                    }

                    // Nationality Name
                    $nationalityName = ($r->nationality == 1) ? "Indian" : "Foreign";

                    // UUID
                    $rateUUID = \Illuminate\Support\Str::uuid()->toString();

                    $rateDetails[] = [
                        "UniqueID" => $rateUUID,
                        "SupplierId" => (int) $r->supplierId,
                        "SupplierName" => $supplierName,
                        "NationalityId" => (int) $r->nationality,
                        "NationalityName" => $nationalityName,
                        "ValidFrom" => $r->fromDate,
                        "ValidTo" => $r->toDate,
                        "CurrencyId" => (int) $r->currencyId,
                        "CurrencyName" => "",
                        "CurrencyConversionName" => "",
                        "IndianAdultEntFee" => (string) $r->adultCost,
                        "IndianChildEntFee" => (string) $r->childCost,
                        "ForeignerAdultEntFee" => (string) $r->adultCost,
                        "ForeignerChildEntFee" => (string) $r->childCost,
                        "TaxSlabId" => (int) $r->gstTax,
                        "TaxSlabName" => "IT",
                        "TaxSlabVal" => "0",
                        "TotalCost" => 0,
                        "Policy" => "",
                        "TAC" => "",
                        "Remarks" => "",
                        "Status" => (string) $r->status,
                        "AddedBy" => 0,
                        "UpdatedBy" => 0,
                        "AddedDate" => now(),
                        "UpdatedDate" => now()
                    ];
                }


                //------------------------------------
                // BUILD RATE JSON ONLY IF DATA EXISTS
                //------------------------------------
                $rateJson = null;

                if (!empty($rateDetails)) {
                    $rateJsonStructure = [
                        "MonumentId" => $user->id,
                        "MonumentUUID" => $uniqueId,
                        "MonumentName" => $user->entranceName,
                        "DestinationID" => $destinationId,
                        "DestinationName" => $destinationName,
                        "CompanyId" => "",
                        "CompanyName" => "",
                        "Header" => $header,
                        "Data" => [
                            [
                                "Total" => count($rateDetails),
                                "RateDetails" => $rateDetails
                            ]
                        ]
                    ];

                    $rateJson = json_encode($rateJsonStructure, JSON_UNESCAPED_UNICODE);

                    // Only run if rateDetailsList has data
                    if (!empty($rateDetails)) {
                        foreach ($rateDetails as $rateItem) {
                            // Extract dates
                            $startDate = Carbon::parse($rateItem['ValidFrom']);
                            $endDate = Carbon::parse($rateItem['ValidTo']);

                            $destinationUniqueID = !empty($destinationId) ? 'DES' . str_pad($destinationId, 6, '0', STR_PAD_LEFT) : '';
                            $supplierUniqueID = !empty($rateItem['SupplierId']) ? 'SUPP' . str_pad($rateItem['SupplierId'], 6, '0', STR_PAD_LEFT) : '';

                            // Loop day-by-day
                            while ($startDate->lte($endDate)) {

                                DB::connection('pgsql')
                                    ->table('sightseeing.monument_search')
                                    ->updateOrInsert(
                                        [
                                            "RateUniqueId" => $rateItem['UniqueID'],  // unique per rate
                                            "MonumentUID" => $uniqueId,
                                            "Date" => $startDate->format("Y-m-d")
                                        ],
                                        [
                                            "Destination" => $destinationUniqueID,
                                            //"RoomBedType"   => json_encode($rateItem['RoomBedType'], JSON_UNESCAPED_UNICODE),
                                            "SupplierUID" => $supplierUniqueID,
                                            "CompanyId" => 0,
                                            "Currency" => $rateItem['CurrencyId'],
                                            "RateJson" => $rateJson,
                                            "Status" => 1,
                                            "AddedBy" => 1,
                                            "UpdatedBy" => 1,
                                            "created_at" => now(),
                                            "updated_at" => now()
                                        ]
                                    );
                                ///update
                                $startDate->addDay(); // next date
                            }
                        }
                    }
                }

                //------------------------------------
                // PREPARE INSERT DATA
                //------------------------------------
                $updateData = [
                    'id' => $user->id,
                    'MonumentName' => $user->entranceName,
                    'Destination' => $destinationId,
                    'TransferType' => $user->transferType,
                    'Default' => $user->isDefault,
                    'Status' => $user->status,
                    'JsonWeekendDays' => $closeDaysnameJson,
                    'UniqueID' => $uniqueId,
                    'AddedBy' => 1,
                    'UpdatedBy' => 1,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                // VERY IMPORTANT:
                // Only add RateJson if data exists
                if (!empty($rateJson)) {
                    $updateData['RateJson'] = $rateJson;
                }

                //------------------------------------
                // INSERT / UPDATE
                //------------------------------------
                DB::connection('pgsql')
                    ->table('sightseeing.monument_master')
                    ->updateOrInsert(
                        ['id' => $user->id],
                        $updateData
                    );
            }

            return [
                'status' => true,
                'message' => 'Monument Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function syncDirectClient()
    {
        try {

            $mysqlUsers = DB::connection('mysql')
                ->table('contactsmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                // ✅ Unique ID
                $uniqueId = !empty($user->id)
                    ? 'DICL' . str_pad($user->id, 6, '0', STR_PAD_LEFT)
                    : null;

                // ✅ FIX TITLE
                $title = match ((int) ($user->contacttitleId ?? 0)) {
                    1 => 'Mr.',
                    2 => 'Mrs.',
                    3 => 'Ms.',
                    default => 'Mr.',
                };

                // ✅ FIX CONTACT TYPE
                $contactType = match ((int) ($user->contactType ?? 0)) {
                    1 => 'Employee',
                    2 => 'B2C',
                    3 => 'Guest',
                    default => 'Guest List',
                };

                // ✅ FIX GENDER (VERY IMPORTANT)
                $rawGender = strtolower(trim($user->gender ?? ''));

                $gender = match ($rawGender) {
                    'Male', 'm' => 'Male',
                    'Female', 'f', 'female' => 'Female',
                    'other', 'o' => 'Other',
                    default => 'Male', // ✅ REQUIRED because Gender is NOT NULL
                };

                // -------------------------------------------------
                // 📞 FETCH PHONE DETAILS (INLINE)
                // -------------------------------------------------
                $phones = DB::connection('mysql')
                    ->table('phonemaster')
                    ->where('sectionType', 'contacts')
                    ->where('masterId', $user->id)
                    ->get();

                // -------------------------------------------------
                // 📧 FETCH EMAIL DETAILS (INLINE)
                // -------------------------------------------------
                $emails = DB::connection('mysql')
                    ->table('emailmaster')
                    ->where('sectionType', 'contacts')
                    ->where('masterId', $user->id)
                    ->get();

                // -------------------------------------------------
                // 🧩 BUILD CONTACTINFO JSON
                // -------------------------------------------------
                $contactInfo = [];
                $max = max($phones->count(), $emails->count());

                for ($i = 0; $i < $max; $i++) {

                    $phone = $phones[$i] ?? null;
                    $email = $emails[$i] ?? null;

                    $contactInfo[] = [
                        'ContactId' => 1,
                        'Contact_Type' => 'Work',
                        'CountryCode' => $phone->countryCode ?? '91',
                        'Mobile' => $phone->phoneNo ?? null,
                        'EmailType' => 'Work',
                        'Email' => $email->email ?? null,
                    ];
                }

                // -------------------------------------------------
                // ⛔ EMPTY ARRAY SAFETY
                // -------------------------------------------------
                $contactInfo = empty($contactInfo) ? null : json_encode($contactInfo);

                DB::connection('pgsql')
                    ->table('others.direct_clients')
                    ->updateOrInsert(
                        ['id' => $user->id], // primary key sync
                        [
                            'Title' => $title,
                            'ContactType' => $contactType,
                            'FirstName' => $user->firstName ?? null,
                            'MiddleName' => $user->middleName ?? null,
                            'LastName' => $user->lastName ?? null,

                            // 🔢 Integer-safe fields
                            'MarketType' => is_numeric($user->marketType ?? null) ? (int) $user->marketType : null,
                            'Nationality' => is_numeric($user->nationality ?? null) ? (int) $user->nationality : null,
                            'Country' => is_numeric($user->countryId ?? null) ? (int) $user->countryId : null,
                            'State' => is_numeric($user->stateId ?? null) ? (int) $user->stateId : null,
                            'City' => is_numeric($user->cityId ?? null) ? (int) $user->cityId : null,

                            // ✅ Fixed Gender
                            'Gender' => $gender,
                            'Contactinfo' => $contactInfo,
                            // ✅ Dates
                            'DOB' => $this->fixDate($user->birthDate ?? null),
                            'AnniversaryDate' => $this->fixDate($user->anniversaryDate ?? null),
                            'TourId' => $user->tourId ?? null,
                            'QueryId' => $user->queryId ?? null,
                            'Remark1' => $user->remark1 ?? null,
                            'EmergencyContactNumber' => $user->emergencyContact ?? null,
                            'Agent' => $user->agentName ?? null,
                            'UniqueId' => $uniqueId,
                            'Status' => is_numeric($user->status ?? null) ? (int) $user->status : 1,

                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Direct Client data synced successfully'
            ];
        } catch (\Exception $e) {

            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function transferTypeSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('transfertypemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('transport.transfer_type_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->name,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Transfer Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function vehicleTypeMasterSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('vehicletypemaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('transport.vehicle_type_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->name,
                            'PaxCapacity' => $user->capacity,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Vehicle Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    // public function supplierContactSync()
    // {
    //     try {
    //         // ✅ Read all data from MySQL
    //         $mysqlUsers = DB::connection('mysql')
    //             ->table('suppliercontactpersonmaster')
    //             ->get();

    //         foreach ($mysqlUsers as $user) {

    //             // ✅ Insert / Update data to PGSQL
    //             DB::connection('pgsql')
    //                 ->table('others.contact_person_master')
    //                 ->updateOrInsert(
    //                     ['id' => $user->id],  // Match by primary key
    //                     [
    //                         //'id'           => $user->id,
    //                         'ParentId'           => $user->corporateId,
    //                         'OfficeName'           => "Head Office",
    //                         //'MetDuring'           => "",
    //                         'Title'           => "",
    //                         'FirstName'           => $user->contactPerson ?? '',
    //                         'LastName'           => $user->lastName ?? '',
    //                         'Email'           => $this->safeTripleDecode($user->email) ?? '',
    //                         'Phone'           => $this->safeTripleDecode($user->phone) ?? '',
    //                         'MobileNo'           => $this->safeTripleDecode($user->phone) ?? '',
    //                         'Designation'           => $user->designation ?? '',
    //                         'Division' => isset($user->division) && is_numeric($user->division)
    //                             ? (int) $user->division
    //                             : null,
    //                         'CountryCode'          => $user->countryCode,
    //                         'type'  => 'Supplier',
    //                         'Status'          => 'Yes',
    //                         //'RPK'  => $user->id,
    //                         'AddedBy'     => 1,
    //                         'UpdatedBy'     => 1,
    //                         'created_at'     => now(),
    //                         'updated_at'     => now(),
    //                     ]
    //                 );
    //         }

    //         return [
    //             'status'  => true,
    //             'message' => 'Supplier Contact Data synced successfully'
    //         ];
    //     } catch (\Exception $e) {
    //         return [
    //             'status'  => false,
    //             'message' => $e->getMessage(),
    //         ];
    //     }
    // }

    public function supplierContactSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('suppliercontactpersonmaster')
                ->get();

            foreach ($mysqlUsers as $user) {

                // ✅ Fallback supplier name if contact person is empty
                $contactPersonName = $user->contactPerson;

                if (empty($contactPersonName)) {
                    $supplier = DB::connection('mysql')
                        ->table('suppliersmaster')
                        ->where('id', $user->corporateId)
                        ->first();

                    $contactPersonName = $supplier->name ?? '';
                }

                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.contact_person_master')
                    ->updateOrInsert(
                        //['id' => $user->id],
                        [
                            'ParentId' => $user->corporateId,
                            'OfficeName' => 'Head Office',
                            'Title' => '',
                            'FirstName' => $contactPersonName,
                            'LastName' => $user->lastName ?? '',
                            'Email' => $this->safeTripleDecode($user->email) ?? '',
                            'Phone' => $this->safeTripleDecode($user->phone) ?? '',
                            'MobileNo' => $this->safeTripleDecode($user->phone) ?? '',
                            'Designation' => $user->designation ?? '',
                            'Division' => isset($user->division) && is_numeric($user->division)
                                ? (int) $user->division
                                : null,
                            'CountryCode' => $user->countryCode,
                            'type' => 'Supplier',
                            'Status' => 'Yes',
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Supplier Contact Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }




    public function marketTypeSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('marketmaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.market_type_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->name,
                            'Status' => $user->status,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Market Type Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    public function nationalitySync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('nationalitymaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.nationality_master')
                    ->updateOrInsert(
                        ['id' => $user->id],  // Match by primary key
                        [
                            'id' => $user->id,
                            'Name' => $user->name,
                            'Status' => $user->status ?? 1,
                            //'RPK'  => $user->id,
                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Nationality Master Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }

    private function pgInt($value, $default = 0)
    {
        return is_numeric($value) ? (int) $value : $default;
    }

    private function pgText($value, $default = '')
    {
        return ($value !== null && $value !== '') ? $value : $default;
    }

    public function itiInfoSync()
    {
        try {
            // ✅ Read all data from MySQL
            $mysqlUsers = DB::connection('mysql')
                ->table('iti_subjectmaster')
                ->get();

            foreach ($mysqlUsers as $user) {


                // ✅ Insert / Update data to PGSQL
                DB::connection('pgsql')
                    ->table('others.itinerary_requirement')
                    ->updateOrInsert(
                        ['id' => $user->id],
                        [
                            'FromDestination' => $this->pgInt($user->fromDestinationId),
                            'ToDestination' => $this->pgInt($user->toDestinationId),

                            'TransferMode' => $this->pgText($user->transferMode),

                            'Title' => $this->pgText($user->otherTitle),

                            'Description' => $this->pgText($user->description),

                            'DrivingDistance' => $this->pgInt($user->driving_distance ?? 0),

                            'Status' => $this->pgInt($user->status, 1),

                            'AddedBy' => 1,
                            'UpdatedBy' => 1,
                            'created_at' => now(),
                            'updated_at' => now(),
                        ]
                    );
            }

            return [
                'status' => true,
                'message' => 'Itinarary Info Data synced successfully'
            ];
        } catch (\Exception $e) {
            return [
                'status' => false,
                'message' => $e->getMessage(),
            ];
        }
    }
}
