import com.viglink.associationrules.{LinkAssociationRules, Utils}
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by alexminnaar on 11/28/16.
  */
class UrlParseSpec extends WordSpec with Matchers {

  "url parser" should {

    "extract the correct param keys" in {

      val testUrl = "?cm_cat=datafeed&cm_pla=tops:women:sweater&cm_ite=moschino_cheap_%26_chic_peplum_sweater:677286&cm_ven=Linkshare&cuid=33001cc076c1d69b5566e7bd13699008"
      Utils.urlParamKeySplitter(testUrl) should equal("?cm_catcm_itecm_placm_vencuid")

    }

    "determine if it is a root domain" in {

      val testRootDomain = "www.example.com"
      val testNotRootDomain = "shop.example.com"

      val isRoot1 = Utils.isRootDomain(testNotRootDomain)

      isRoot1 should equal(false)

      val isRoot2 = Utils.isRootDomain(testRootDomain)

      isRoot2 should equal(true)

    }

  }

}
