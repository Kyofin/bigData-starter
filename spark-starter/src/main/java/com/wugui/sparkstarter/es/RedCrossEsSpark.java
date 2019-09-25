package com.wugui.sparkstarter.es;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-09-09 15:31
 **/
public class RedCrossEsSpark {

    public static final String JDBC_URL_PHOENIX = "jdbc:phoenix:cdh01:2181";

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("hbase2Es")
                .master("local")
                .getOrCreate();

        readOutPatientRecordFromHbase(sparkSession);

        readInPatientRecordFromHbase(sparkSession);

        Dataset<Row> outPatientRecord = sparkSession.sql("select \n" +
                "snr.reg_date,snr.visit_reason,snr.op_number,\n" +
                "orr.reg_date,orr.dep_name,orr.age,orr.age_month,orr.pre_wm_diagnosis_code,orr.pre_wm_diagnosis_name,orr.pre_tcm_diagnosis_code,orr.pre_tcm_diagnosis_name,orr.pre_tcm_syndrome_code,orr.pre_tcm_syndrome_name,orr.auxiliary_examination_results,orr.auxiliary_examination_project,orr.history_allergic,orr.history_allergic_sign,orr.past_history,orr.present_history,orr.chief_complaint,orr.medical_order_type_code,orr.medical_program_content, orr.medical_order_issue_time,orr.pre_diagnosis_sign,\n" +
                "pbi.empi,pbi.health_card_number,pbi.patient_name,\n" +
                "\n" +
                "eor.pre_wm_diagnosis_code,eor.pre_wm_diagnosis_name,eor.auxiliary_examination_results,eor.auxiliary_examination_project,eor.history_allergic,eor.history_allergic_sign,eor.record_course_emergency_observation,eor.emergency_rescue_records,eor.record_date,eor.past_history,eor.intervention_name,eor.reg_date,eor.dep_name,eor.age,eor.age_month,eor.rescue_end_date,eor.rescue_start_date,eor.income_observation_room_date,eor.surgery_code,eor.surgery_operation_times,eor.surgery_operation_method,eor.surgery_operation_name,eor.surgery_operation_part_name,eor.physically_checkup,eor.present_history,eor.gender_code,eor.medical_order_type_code,eor.medical_program_content,eor.chief_complaint,eor.attention,\n" +
                "\n" +
                "ir.specimen_status,ir.sample_type,ir.sample_fluid,ir.operational_coding,ir.operating_part_code,ir.operating_number,ir.operating_method_description,ir.operating_name,ir.patient_type_code,ir.disease_diagnostic_code,ir.exam_report_note,ir.exam_report_number,ir.exam_report_number_obj,ir.exam_report_number_sub,ir.exam_report_date,ir.exam_quantitative_result,ir.exam_quantitative_mea_unit,ir.exam_result_code,ir.exam_type,ir.exam_item_code,ir.exam_method_name,ir.intervention_name,ir.anesth_type_code,ir.anesth_observation_results,ir.treatment_process_describe,ir.symptom_start_time,ir.symptom_diagnostic_desc,ir.symptom_stop_time,\n" +
                "\n" +
                "ar.sample_type,ar.specimen_status,ar.application_formno,ar.disease_diagnostic_code,ar.test_report_remark,ar.test_report_remark_number,ar.test_report_result,ar.test_specimen_number,ar.test_quantitative_result,ar.test_quantitative_result_unit,ar.test_method_name,ar.test_technician_signature,ar.test_result_code,ar.test_type,ar.age,ar.age_month,\n" +
                "\n" +
                "wmp.prescription_number,wmp.presc_remarks_information,wmp.presc_open_date,wmp.presc_open_doctor_signature,wmp.presc_drugs_group_number,wmp.medication_specifications,wmp.dosage_form_code,wmp.medication_name,wmp.medication_use_dosage,wmp.medication_use_dosage_unit,wmp.medication_use_frequency_code,wmp.total_dose_medication,wmp.medication_use_means_code\n" +
                "from \n" +
                "outpatient_record as orr\n" +
                "left join patient_basic_information as pbi on\torr.patient_id = pbi.patient_id\n" +
                "left join emergency_observation_record as eor on orr.op_number = eor.op_number\n" +
                "left join inspection_record as ir on orr.op_number = ir.op_number\n" +
                "LEFT JOIN assay_record AS ar ON orr.op_number = ar.op_number\n" +
                "LEFT JOIN western_medicine_prescription AS wmp ON orr.op_number = wmp.op_number\n" +
                "left join sanitation_negligence_record AS snr on orr.op_number = snr.op_number\n" +
                "WHERE orr.op_number IS NOT NULL");

        outPatientRecord.show();

        Dataset<Row> inPatientRecord = sparkSession.sql("SELECT\n" +
                "snr.admisson_no,snr.discharge_date,snr.into_date,\n" +
                "pbi.empi,pbi.health_card_number,pbi.patient_name, \n" +
                "\n" +
                "ir.specimen_status,ir.sample_type,ir.sample_fluid,ir.operational_coding,ir.operating_part_code,ir.operating_number,ir.operating_method_description,ir.operating_name,ir.patient_type_code,ir.disease_diagnostic_code,ir.exam_report_note,ir.exam_report_number,ir.exam_report_number_obj,ir.exam_report_number_sub,ir.exam_report_date,ir.exam_quantitative_result,ir.exam_quantitative_mea_unit,ir.exam_result_code,ir.exam_type,ir.exam_item_code,ir.exam_method_name,ir.intervention_name,ir.anesth_type_code,ir.anesth_observation_results,ir.treatment_process_describe,ir.symptom_start_time,ir.symptom_diagnostic_desc,ir.symptom_stop_time,\n" +
                "\n" +
                "ar.sample_type,ar.specimen_status,ar.application_formno,ar.disease_diagnostic_code,ar.test_report_remark,ar.test_report_remark_number,ar.test_report_result,ar.test_specimen_number,ar.test_quantitative_result,ar.test_quantitative_result_unit,ar.test_method_name,ar.test_technician_signature,ar.test_result_code,ar.test_type,ar.age,ar.age_month,\n" +
                "\n" +
                "cn.ward_bed_number,cn.ward_room_number,cn.medical_records_sampling,cn.ward_name,cn.auxiliary_examination_results,cn.lack_consultation,cn.name_consultation_department,cn.consultation_type,cn.purpose_consultation,cn.consultation_date,cn.consult_reason_desc,cn.diag_code,cn.diagnosis_name,cn.treatment_process_describe,cn.medical_treatment_name\n" +
                "from\n" +
                "hospitalized_case_index as hci\n" +
                "left join patient_basic_information as pbi on hci.patient_id = pbi.patient_id\n" +
                "left join inspection_record as ir on hci.admisson_no = ir.admisson_no\n" +
                "left join assay_record as ar on hci.admisson_no = ar.admisson_no\n" +
                "left join consultation_note as cn on hci.admisson_no = cn.admisson_no\n" +
                "left join sanitation_negligence_record as snr on hci.admisson_no = snr.admisson_no");

        inPatientRecord.show();


        // 写入es
        sparkSession.sparkContext().conf().set("es.index.auto.create", "true")
                .set("es.nodes", "172.19.4.171")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");


        JavaEsSparkSQL.saveToEs(outPatientRecord,"honghui/menzhen");
        JavaEsSparkSQL.saveToEs(inPatientRecord,"honghui/zhuyuan");
    }

    private static void readInPatientRecordFromHbase(SparkSession sparkSession) throws AnalysisException {
        // 使用phoenix jdbc连接驱动读取数据
        Dataset<Row> sanitationNegligenceRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", JDBC_URL_PHOENIX)
                .option("dbtable", "\"gzhonghui\".\"sanitation_negligence_record\"")
                .load();
        sanitationNegligenceRecord.createOrReplaceTempView("sanitation_negligence_record");

        Dataset<Row> hospitalizedCaseIndex = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"hospitalized_case_index\"")
                .load();
        hospitalizedCaseIndex.createOrReplaceTempView("hospitalized_case_index");


        Dataset<Row> patientBasicInformation = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"patient_basic_information\"")
                .load();
        patientBasicInformation.createOrReplaceTempView("patient_basic_information");


        Dataset<Row> inspectionRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"inspection_record\"")
                .load();
        inspectionRecord.createOrReplaceTempView("inspection_record");


        Dataset<Row> assayRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"assay_record\"")
                .load();
        assayRecord.createOrReplaceTempView("assay_record");


        Dataset<Row> consultationNote = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"consultation_note\"")
                .load();
        consultationNote.createOrReplaceTempView("consultation_note");
    }

    private static void readOutPatientRecordFromHbase(SparkSession sparkSession) throws AnalysisException {
        // 使用phoenix jdbc连接驱动读取数据
        Dataset<Row> sanitationNegligenceRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"sanitation_negligence_record\"")
                .load();
        sanitationNegligenceRecord.createOrReplaceTempView("sanitation_negligence_record");


        Dataset<Row> patientBasicInformation = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"patient_basic_information\"")
                .load();
        patientBasicInformation.createOrReplaceTempView("patient_basic_information");


        Dataset<Row> outpatientRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"outpatient_record\"")
                .load();
        outpatientRecord.createOrReplaceTempView("outpatient_record");


        Dataset<Row> emergencyObservationRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"emergency_observation_record\"")
                .load();
        emergencyObservationRecord.createOrReplaceTempView("emergency_observation_record");


        Dataset<Row> inspectionRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"inspection_record\"")
                .load();
        inspectionRecord.createOrReplaceTempView("inspection_record");


        Dataset<Row> assayRecord = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"assay_record\"")
                .load();
        assayRecord.createOrReplaceTempView("assay_record");


        Dataset<Row> westernMedicinePrescription = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("phoenix.schema.isNamespaceMappingEnabled", "true")
                .option("url", "jdbc:phoenix:cdh01:2181")
                .option("dbtable", "\"gzhonghui\".\"western_medicine_prescription\"")
                .load();
        westernMedicinePrescription.createOrReplaceTempView("western_medicine_prescription");
    }
}
