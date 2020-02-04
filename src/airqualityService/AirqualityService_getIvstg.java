package airqualityService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class AirqualityService_getIvstg {
	
	 final static Logger logger = Logger.getLogger(AirqualityService_getIvstg .class);

	// 대기질 정보 조회 - 조사속성조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		//System.out.println("오픈api 시작");
		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("airquality_getIvstg_url");
		String service_key = JsonParser.getProperty("airquality_service_key");
		
		// step 0.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// step 1.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("AirqualityService_getIvstg_" + strDate + ".dat");

		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			pw.write("mgtNo"); //사업 코드
			pw.write("|^");
			pw.write("ivstgGb"); //조사구분
			pw.write("|^");
			pw.write("ivstgSpotNm"); //조사지점명
			pw.write("|^");
			pw.write("adres"); //주소
			pw.write("|^");
			pw.write("xcnts"); //X좌표 
			pw.write("|^");
			pw.write("ydnts"); //Y좌표
			pw.write("|^");
			pw.write("ivstgOdr"); //조사차수
			pw.write("|^");
			pw.write("ivstgBgnde"); //조사시작일
			pw.write("|^");
			pw.write("ivstgEndde"); //조사종료일
			pw.write("|^");
			pw.write("pm10Val"); //미세먼지(10)
			pw.write("|^");
			pw.write("pm25Val"); //미세먼지(2.5)
			pw.write("|^");
			pw.write("no2Val"); //이산화질소
			pw.write("|^");
			pw.write("so2Val"); //아황산가스
			pw.write("|^");
			pw.write("coVal"); //일산화탄소
			pw.write("|^");
			pw.write("o3Val"); //오존
			pw.write("|^");
			pw.write("pbVal"); //납
			pw.write("|^");
			pw.write("bzVal"); //벤젠
			pw.write("|^");
			pw.write("hclVal"); //염화수소(HCL)
			pw.write("|^");
			pw.write("hgVal"); //수은(Hg)
			pw.write("|^");
			pw.write("niVal"); //니켈(Ni)
			pw.write("|^");
			pw.write("cr6Val"); //6가크롬(Cr6+)
			pw.write("|^");
			pw.write("cdVal"); //카드뮴(Cd)
			pw.write("|^");
			pw.write("asVal"); //비소(As)
			pw.write("|^");
			pw.write("hchoVal"); //포름알데히드
			pw.write("|^");
			pw.write("vcVal"); //염화비닐
			pw.write("|^");
			pw.write("dioxinVal"); //다이옥신
			pw.write("|^");
			pw.write("beVal"); //베릴륨(Be)
			pw.write("|^");
			pw.write("ebVal"); //에틸벤젠
			pw.write("|^");
			pw.write("c6h14Val"); //n-헥산
			pw.write("|^");
			pw.write("c6h12Val"); //시클로헥산
			pw.write("|^");
			pw.write("deVal"); //1,-2디클로로에탄
			pw.write("|^");
			pw.write("cfVal"); //클로로포름
			pw.write("|^");
			pw.write("tceVal"); //트리클로로에틸렌
			pw.write("|^");
			pw.write("ctVal"); //사염화탄소
			pw.write("|^");
			pw.write("hcnVal"); //시안화수소
			pw.write("|^");
			pw.write("gmenoPm1024hr"); //공사시 PM-10_24시간평균
			pw.write("|^");
			pw.write("gmenoPm10Year"); //공사시 PM-10_연평균
			pw.write("|^");
			pw.write("gmenoPm2524hr"); //공사시 PM-2.5_24시간평균
			pw.write("|^");
			pw.write("gmenoPm25Year"); //공사시 PM-2.5_연평균
			pw.write("|^");
			pw.write("gmenoNo21hr"); //공사시 NO2_1시간평균
			pw.write("|^");
			pw.write("gmenoNo224hr"); //공사시 NO2_24시간평균
			pw.write("|^");
			pw.write("gmenoNo2Year"); //공사시 NO2_연평균
			pw.write("|^");
			pw.write("umenoPm1024hr"); //운영시 PM-10_24시간평균
			pw.write("|^");
			pw.write("umenoPm10Year"); //운영시 PM-10_연평균
			pw.write("|^");
			pw.write("umenoPm2524hr"); //운영시 PM-2.5_24시간평균
			pw.write("|^");
			pw.write("umenoPm25Year"); //운영시 PM-2.5_연평균
			pw.write("|^");
			pw.write("umenoSo21hr"); //운영시 SO2_1시간평균
			pw.write("|^");
			pw.write("umenoSo224hr"); //운영시 SO2_24시간평균
			pw.write("|^");
			pw.write("umenoSo2Year"); //운영시 SO2_연평균
			pw.write("|^");
			pw.write("umenoNo21hr"); //운영시 NO2_1시간평균
			pw.write("|^");
			pw.write("umenoNo224hr"); //운영시 NO2_24시간평균
			pw.write("|^");
			pw.write("umenoNo2Year"); //운영시 NO2_연평균
			pw.write("|^");
			pw.write("umenoCo1hr"); //운영시 CO_1시간평균
			pw.write("|^");
			pw.write("umenoCo8hr"); //운영시 CO_8시간평균
			pw.write("|^");
			pw.write("envrnGrad"); //환경기준등급
			pw.println();
			pw.flush();
			pw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		// step 2.오픈 api를 이용해 json 형태로 받아 옴 (요청 파라미터 횟수만큼 반복)

		for (int t = 0; t < businnessCodeList.size(); t++) {

			mgtNo = businnessCodeList.get(t).toString();
			
			String json = "";

			json = JsonParser.parseJson(service_url, service_key, mgtNo);

			// step 3.필요에 맞게 파싱

			try {
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(json);
				JSONObject response = (JSONObject) obj.get("response");

				// response는 결과값 코드와 메시지를 가지는 header와 데이터 부분인 body로 구분
				JSONObject body = (JSONObject) response.get("body");

				JSONArray ivstgGbs = (JSONArray) body.get("ivstgGbs");

				// 공통으로 갖는 사업 코드
				for (int i = 0; i < ivstgGbs.size(); i++) {
					JSONObject ivstgGb_Json = (JSONObject) ivstgGbs.get(i);

					String ivstgGb_str = ivstgGb_Json.get("ivstgGb").toString(); //조사구분

					JSONArray ivstgs = (JSONArray) ivstgGb_Json.get("ivstgs");

					for (int r = 0; r < ivstgs.size(); r++) {

						JSONObject ivstg = (JSONObject) ivstgs.get(r);

						String ivstg_adres_str = ""; //주소 
						String ivstg_ivstgSpotNm_str = ""; //조사지점명
						String ivstg_xcnts_str = ""; //X좌표 
						String ivstg_ydnts_str = ""; //Y좌표

						if (ivstg.get("adres") != null) {
							ivstg_adres_str = ivstg.get("adres").toString();
						} else {
							ivstg_adres_str = "";
						}

						if (ivstg.get("ivstgSpotNm") != null) {
							ivstg_ivstgSpotNm_str = ivstg.get("ivstgSpotNm").toString();
						} else {
							ivstg_ivstgSpotNm_str = "";
						}

						if (ivstg.get("xcnts") != null) {
							ivstg_xcnts_str = ivstg.get("xcnts").toString();
						} else {
							ivstg_xcnts_str = "";
						}

						if (ivstg.get("ydnts") != null) {
							ivstg_ydnts_str = ivstg.get("ydnts").toString();
						} else {
							ivstg_ydnts_str = "";
						}

						JSONArray odrs = (JSONArray) ivstg.get("odrs");

						for (int f = 0; f < odrs.size(); f++) {

							JSONObject odr = (JSONObject) odrs.get(f);

							Set<String> key = odr.keySet();

							Iterator<String> iter = key.iterator();

							// 3개는 공통적인 바깥 배열의 값이므로 Iterator로 받아 올 수 없다..
							// String ivstgGb = "";
							// String ivstgSpotNm = "";
							// String adres = "";
							String ivstgOdr = "";  //조사차수
							String ivstgBgnde = ""; //조사시작일
							String ivstgEndde = ""; //조사종료일
							String pm10Val = ""; //미세먼지(10)
							String pm25Val = ""; //미세먼지(2.5)
							String no2Val = ""; //이산화질소
							String so2Val = ""; //아황산가스
							String coVal = ""; //일산화탄소
							String o3Val = ""; //오존
							String pbVal = ""; //납
							String bzVal = ""; //벤젠
							String hclVal = ""; //염화수소(HCL)
							String hgVal = ""; //수은(Hg)
							String niVal = ""; //니켈(Ni)
							String cr6Val = ""; //6가크롬(Cr6+)
							String cdVal = ""; //카드뮴(Cd)
							String asVal = ""; //비소(As)
							String hchoVal = ""; //포름알데히드
							String vcVal = ""; //염화비닐
							String dioxinVal = ""; //다이옥신
							String beVal = ""; //베릴륨(Be)
							String ebVal = ""; //에틸벤젠
							String c6h14Val = ""; //n-헥산
							String c6h12Val = ""; //시클로헥산
							String deVal = ""; //1,-2디클로로에탄
							String cfVal = ""; //클로로포름
							String tceVal = ""; //트리클로로에틸렌
							String ctVal = ""; //사염화탄소
							String hcnVal = ""; //시안화수소
							String gmenoPm1024hr = ""; //공사시 PM-10_24시간평균
							String gmenoPm10Year = ""; //공사시 PM-10_연평균
							String gmenoPm2524hr = ""; //공사시 PM-2.5_24시간평균
							String gmenoPm25Year = ""; //공사시 PM-2.5_연평균
							String gmenoNo21hr = ""; //공사시 NO2_1시간평균
							String gmenoNo224hr = ""; //공사시 NO2_24시간평균
							String gmenoNo2Year = ""; //공사시 NO2_연평균
							String umenoPm1024hr = ""; //운영시 PM-10_24시간평균
							String umenoPm10Year = ""; //운영시 PM-10_연평균
							String umenoPm2524hr = ""; //운영시 PM-2.5_24시간평균
							String umenoPm25Year = ""; //운영시 PM-2.5_연평균
							String umenoSo21hr = ""; //운영시 SO2_1시간평균
							String umenoSo224hr = ""; //운영시 SO2_24시간평균
							String umenoSo2Year = ""; //운영시 SO2_연평균
							String umenoNo21hr = ""; //운영시 NO2_1시간평균
							String umenoNo224hr = ""; //운영시 NO2_24시간평균
							String umenoNo2Year = ""; //운영시 NO2_연평균
							String umenoCo1hr = ""; //운영시 CO_1시간평균
							String umenoCo8hr = ""; //운영시 CO_8시간평균
							String envrnGrad = ""; //환경기준등급  
							

							while (iter.hasNext()) {
								String keyname = iter.next();

								//System.out.println(keyname + ":" + odr.get(keyname).toString());

								// 3개는 공통적인 바깥 배열의 값이므로 Iterator로 받아 올 수 없다..
								/*
								 * if(keyname.equals("ivstgGb")){ ivstgGb =
								 * odr.get(keyname).toString(); }
								 * if(keyname.equals("ivstgSpotNm")){
								 * ivstgSpotNm = odr.get(keyname).toString(); }
								 * if(keyname.equals("adres")){ adres =
								 * odr.get(keyname).toString(); }
								 */
								if (keyname.equals("ivstgOdr")) {
									ivstgOdr = odr.get(keyname).toString();
								}
								if (keyname.equals("ivstgBgnde")) {
									ivstgBgnde = odr.get(keyname).toString();
								}
								if (keyname.equals("ivstgEndde")) {
									ivstgEndde = odr.get(keyname).toString();
								}
								if (keyname.equals("pm10Val")) {
									pm10Val = odr.get(keyname).toString();
								}
								if (keyname.equals("pm25Val")) {
									pm25Val = odr.get(keyname).toString();
								}
								if (keyname.equals("no2Val")) {
									no2Val = odr.get(keyname).toString();
								}
								if (keyname.equals("so2Val")) {
									so2Val = odr.get(keyname).toString();
								}
								if (keyname.equals("coVal")) {
									coVal = odr.get(keyname).toString();
								}
								if (keyname.equals("o3Val")) {
									o3Val = odr.get(keyname).toString();
								}
								if (keyname.equals("pbVal")) {
									pbVal = odr.get(keyname).toString();
								}
								if (keyname.equals("bzVal")) {
									bzVal = odr.get(keyname).toString();
								}
								if (keyname.equals("hclVal")) {
									hclVal = odr.get(keyname).toString();
								}
								if (keyname.equals("hgVal")) {
									hgVal = odr.get(keyname).toString();
								}
								if (keyname.equals("niVal")) {
									niVal = odr.get(keyname).toString();
								}
								if (keyname.equals("cr6Val")) {
									cr6Val = odr.get(keyname).toString();
								}
								if (keyname.equals("cdVal")) {
									cdVal = odr.get(keyname).toString();
								}
								if (keyname.equals("asVal")) {
									asVal = odr.get(keyname).toString();
								}
								if (keyname.equals("hchoVal")) {
									hchoVal = odr.get(keyname).toString();
								}
								if (keyname.equals("vcVal")) {
									vcVal = odr.get(keyname).toString();
								}
								if (keyname.equals("dioxinVal")) {
									dioxinVal = odr.get(keyname).toString();
								}
								if (keyname.equals("beVal")) {
									beVal = odr.get(keyname).toString();
								}
								if (keyname.equals("ebVal")) {
									ebVal = odr.get(keyname).toString();
								}
								if (keyname.equals("c6h14Val")) {
									c6h14Val = odr.get(keyname).toString();
								}
								if (keyname.equals("c6h12Val")) {
									c6h12Val = odr.get(keyname).toString();
								}
								if (keyname.equals("deVal")) {
									deVal = odr.get(keyname).toString();
								}
								if (keyname.equals("cfVal")) {
									cfVal = odr.get(keyname).toString();
								}
								if (keyname.equals("tceVal")) {
									tceVal = odr.get(keyname).toString();
								}
								if (keyname.equals("ctVal")) {
									ctVal = odr.get(keyname).toString();
								}
								if (keyname.equals("hcnVal")) {
									hcnVal = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoPm1024hr")) {
									gmenoPm1024hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoPm10Year")) {
									gmenoPm10Year = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoPm2524hr")) {
									gmenoPm2524hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoPm25Year")) {
									gmenoPm25Year = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo21hr")) {
									gmenoNo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo224hr")) {
									gmenoNo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo2Year")) {
									gmenoNo2Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm1024hr")) {
									umenoPm1024hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm10Year")) {
									umenoPm10Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm2524hr")) {
									umenoPm2524hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm25Year")) {
									umenoPm25Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo21hr")) {
									umenoSo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo224hr")) {
									umenoSo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo2Year")) {
									umenoSo2Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo21hr")) {
									umenoNo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo224hr")) {
									umenoNo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo2Year")) {
									umenoNo2Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoCo1hr")) {
									umenoCo1hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoCo8hr")) {
									umenoCo8hr = odr.get(keyname).toString();
								}
								if (keyname.equals("envrnGrad")) {
									envrnGrad = odr.get(keyname).toString();
								}
							}

							// step 4. 파일에 쓰기
							try {
								PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

								pw.write(mgtNo); //사업코드
								pw.write("|^");
								pw.write(ivstgGb_str); //조사구분
								pw.write("|^");
								pw.write(ivstg_ivstgSpotNm_str); //조사지점명
								pw.write("|^");
								pw.write(ivstg_adres_str); //주소
								pw.write("|^");
								pw.write(ivstg_xcnts_str); //X좌표
								pw.write("|^");
								pw.write(ivstg_ydnts_str); //Y좌표
								pw.write("|^");
								pw.write(ivstgOdr); //조사차수 
								pw.write("|^");
								pw.write(ivstgBgnde); //조사시작일
								pw.write("|^");
								pw.write(ivstgEndde); //조사종료일
								pw.write("|^");
								pw.write(pm10Val); //미세먼지(10)
								pw.write("|^");
								pw.write(pm25Val); //미세먼지(2.5)
								pw.write("|^");
								pw.write(no2Val); //이산화질소
								pw.write("|^");
								pw.write(so2Val); //아황산가스
								pw.write("|^");
								pw.write(coVal); //일산화탄소
								pw.write("|^");
								pw.write(o3Val); //오존
								pw.write("|^");
								pw.write(pbVal); //납
								pw.write("|^");
								pw.write(bzVal); //벤젠
								pw.write("|^");
								pw.write(hclVal); //염화수소(HCL)
								pw.write("|^");
								pw.write(hgVal); //수은(Hg)
								pw.write("|^");
								pw.write(niVal); //니켈(Ni)
								pw.write("|^");
								pw.write(cr6Val); //6가크롬(Cr6+)
								pw.write("|^");
								pw.write(cdVal); //카드뮴(Cd)
								pw.write("|^");
								pw.write(asVal); //비소(As)
								pw.write("|^");
								pw.write(hchoVal); //포름알데히드
								pw.write("|^");
								pw.write(vcVal); //염화비닐
								pw.write("|^");
								pw.write(dioxinVal); //다이옥신
								pw.write("|^");
								pw.write(beVal); //베릴륨(Be)
								pw.write("|^");
								pw.write(ebVal); //에틸벤젠
								pw.write("|^");
								pw.write(c6h14Val); //n-헥산
								pw.write("|^");
								pw.write(c6h12Val); //시클로헥산
								pw.write("|^");
								pw.write(deVal); //1,-2디클로로에탄
								pw.write("|^");
								pw.write(cfVal); //클로로포름
								pw.write("|^");
								pw.write(tceVal); //트리클로로에틸렌
								pw.write("|^");
								pw.write(ctVal); //사염화탄소
								pw.write("|^");
								pw.write(hcnVal); //시안화수소
								pw.write("|^");
								pw.write(gmenoPm1024hr); //공사시 PM-10_24시간평균
								pw.write("|^");
								pw.write(gmenoPm10Year); //공사시 PM-10_연평균
								pw.write("|^");
								pw.write(gmenoPm2524hr); //공사시 PM-2.5_24시간평균
								pw.write("|^");
								pw.write(gmenoPm25Year); //공사시 PM-2.5_연평균
								pw.write("|^");
								pw.write(gmenoNo21hr); //공사시 NO2_1시간평균
								pw.write("|^");
								pw.write(gmenoNo224hr); //공사시 NO2_24시간평균
								pw.write("|^");
								pw.write(gmenoNo2Year); //공사시 NO2_연평균
								pw.write("|^");
								pw.write(umenoPm1024hr); //운영시 PM-10_24시간평균
								pw.write("|^");
								pw.write(umenoPm10Year); //운영시 PM-10_연평균
								pw.write("|^");
								pw.write(umenoPm2524hr); //운영시 PM-2.5_24시간평균
								pw.write("|^");
								pw.write(umenoPm25Year); //운영시 PM-2.5_연평균
								pw.write("|^");
								pw.write(umenoSo21hr); //운영시 SO2_1시간평균
								pw.write("|^");
								pw.write(umenoSo224hr); //운영시 SO2_24시간평균
								pw.write("|^");
								pw.write(umenoSo2Year); //운영시 SO2_연평균
								pw.write("|^");
								pw.write(umenoNo21hr); //운영시 NO2_1시간평균
								pw.write("|^");
								pw.write(umenoNo224hr); //운영시 NO2_24시간평균
								pw.write("|^");
								pw.write(umenoNo2Year); //운영시 NO2_연평균
								pw.write("|^");
								pw.write(umenoCo1hr); //운영시 CO_1시간평균
								pw.write("|^");
								pw.write(umenoCo8hr); //운영시 CO_8시간평균
								pw.write("|^");
								pw.write(envrnGrad); //환경기준등급
								pw.println();
								pw.flush();
								pw.close();

							} catch (IOException e) {
								e.printStackTrace();
							}

						}
					}

					//System.out.println("next");
					logger.info("mgtNo :" + mgtNo);
					// 0.2초간 중지시킨다. (그냥 진행하면 응답 서버 에러 가능성)
					Thread.sleep(200);
				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.debug("mgtNo :" + mgtNo);
				// 패스하고 반복문 상단부터 재시작
				continue;
			}

		}
		logger.info("parsing complete!");
	}

}
