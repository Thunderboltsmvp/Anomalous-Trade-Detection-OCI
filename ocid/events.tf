## This configuration was generated by terraform-provider-oci

resource oci_events_rule export_ratd_file_arrival_bronze_rule {
  actions {
    actions {
      action_type = "ONS"
      #description = <<Optional value not found in discovery>>
      #function_id = <<Optional value not found in discovery>>
      is_enabled = "true"
      #stream_id = <<Optional value not found in discovery>>
      topic_id = oci_ons_notification_topic.export_ratd_email_notify_topic.id
    }
  }
  compartment_id = var.compartment_ocid
  condition      = "{\"eventType\":[\"com.oraclecloud.objectstorage.createobject\"],\"data\":{}}"
  defined_tags = {
  }
  description  = "File arrival bronze rule"
  display_name = "ratd_file_arrival_bronze_rule"
  freeform_tags = {
  }
  is_enabled = "true"
}

