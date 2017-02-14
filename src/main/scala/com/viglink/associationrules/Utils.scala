package com.viglink.associationrules

import java.net.URL


object Utils {


  //check if authority is root domain (i.e. starts with "www") or not
  def isRootDomain(authority: String): Boolean = {

    authority
      .split("\\.")
      .head
    match {
      case "www" => true
      case _ => false
    }

  }

  //split url by param keys (i.e. &foo=bar)
  def urlParamKeySplitter(url: String): String = {

    //extract param keys of form '&blah='
    if (url != null) {
      url
        .split("&")
        .filter(p => p.contains("=") && p.length() > 1)
        .map(_.split("=")(0))
        .sorted
        .mkString("|")
    }
    else {
      "NO_PARAMS"
    }
  }

  //Tokenize a url into (protocol, authority, port, path, query, filename, ref)
  def tokenizeUrl(affiliateUrl: String, originalUrl: String): Array[String] = {

    try {
      val aUrl = new URL(affiliateUrl)

      val query = aUrl.getQuery
      val protocol = aUrl.getProtocol
      val authority = aUrl.getAuthority
      val port = aUrl.getPort
      val path = aUrl.getPath
      val file = aUrl.getFile
      val ref = aUrl.getRef

      val oUrl = new URL(originalUrl)
      val oQuery = oUrl.getPath

      Array(
        "isRoot_" + isRootDomain(authority),
        "orig_param_keys_" + urlParamKeySplitter(oQuery),
        "aff_param_keys_" + urlParamKeySplitter(query),
        if (protocol == null) "no_protocol" else "protocol_" + protocol,
        if (authority == null) "no_authority" else "authority_" + authority,
        if (port == -1) "no_port" else "port_" + port.toString,
        if (path == null) "no_path" else "path_" + path,
        if (query == null) "no_query" else "query_" + query,
        if (file == null) "no_file" else "filename_" + file,
        if (ref == null) "no_ref" else "ref_" + ref
      )
    }
    catch {
      //this happens because of table headers from hive query
      case e: java.net.MalformedURLException => Array("row1_header",
        "row2_header",
        "row3_header",
        "row4_header",
        "row5_header",
        "row6_header",
        "row7_header",
        "row8_header",
        "row9_header")
    }
  }

  //transform raw rows into feature vectors [isRootDomain, paramKeys, protocol, authority, port, path, query, file, ref,
  // sameUrl, ip, browserInfo, country, device_type, network_id, conversionY/N]
  def row2Features(rawRow: String): Array[String] = {
    val row = rawRow.split("\t")
    val tokenizedAffUrl = tokenizeUrl(row(1), row(0))

    //if(row(7)=="network_id_26") println(row)

    //if(row(7)== "179" && !tokenizedAffUrl(3).contains("viglink") && row(8)=="0") println(row.toList)
    tokenizedAffUrl ++ Array("sameurl_" + row(2),
      "ip_" + row(3),
      if (row(4) == "NULL") "no_browser_info" else "browser_" + row(4),
      "country_" + row(5),
      "device_type_" + row(6),
      "network_id_" + row(7),
      "merchant_id_" + row(8),
      if (row(9) == "0") "no_conversion" else "conversion")
  }


  //we want to remove specific examples that are invalid
  def validityFilter(example: Array[String]): Boolean = {

    val authority = example(3)
    val networkId = example(14)

    authority != "authority_www.amazon.com" &&
      authority != "authority_viglink.pgpartner.com" &&
      example(14) != "network_id_1" &&
      example(9) != "sameurl_1" &&
      authority != "authority_optimize.viglink.com" &&
      authority != "authority_viglink.go2cloud.org" &&
      authority != "authority_tracking.shopstylers.com" &&
      networkId != "network_id_43" &&
      networkId != "network_id_44" &&
      networkId != "network_id_45"
  }


  def originalUrlFeatures(rawRow: String): Array[String] = {
    val row = rawRow.split("\t")


    val originalUrl = row(0)
    if (originalUrl != "originaloutgoingurl") {

      //print(originalUrl)

      val aUrl = new URL(originalUrl)

      val query = aUrl.getQuery
      val protocol = aUrl.getProtocol
      val authority = aUrl.getAuthority
      val port = aUrl.getPort
      val path = aUrl.getPath
      val file = aUrl.getFile
      val ref = aUrl.getRef

      Array("authority_" + authority, "filename_" + file, if (row(8) == "0") "no_conversion" else "conversion")

    }
    else {
      Array("1_n/a", "2_n/a", "3_n/a", "4_n/a")
    }


  }


}
