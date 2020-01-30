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

		File file = new File("test1_" + strDate + ".txt");

		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			pw.write("mgtNo");
			pw.write("|^");
			pw.write("ivstgGb");
			pw.write("|^");
			pw.write("ivstgSpotNm");
			pw.write("|^");
			pw.write("adres");
			pw.write("|^");
			pw.write("xcnts");
			pw.write("|^");
			pw.write("ydnts");
			pw.write("|^");
			pw.write("coVal");
			pw.write("|^");
			pw.write("ivstgEndde");
			pw.write("|^");
			pw.write("pm10Val");
			pw.write("|^");
			pw.write("pbVal");
			pw.write("|^");
			pw.write("no2Val");
			pw.write("|^");
			pw.write("o3Val");
			pw.write("|^");
			pw.write("so2Val");
			pw.write("|^");
			pw.write("ivstgBgnde");
			pw.write("|^");
			pw.write("ivstgOdr");
			pw.write("|^");
			pw.write("envrnGrad");
			pw.write("|^");
			// 두번째 배열은 컬럼값이 다름
			pw.write("umenoNo224hr");
			pw.write("|^");
			pw.write("umenoCo1hr");
			pw.write("|^");
			pw.write("umenoNo21hr");
			pw.write("|^");
			pw.write("umenoSo21hr");
			pw.write("|^");
			pw.write("umenoSo2Year");
			pw.write("|^");
			pw.write("umenoPm10Year");
			pw.write("|^");
			pw.write("umenoCo8hr");
			pw.write("|^");
			pw.write("umenoPm1024hr");
			pw.write("|^");
			pw.write("umenoSo224hr");
			pw.write("|^");
			pw.write("umenoNo2Year");
			pw.write("|^");
			pw.write("gmenoNo21hr");
			pw.write("|^");
			pw.write("gmenoPm10Year");
			pw.write("|^");
			pw.write("gmenoNo224hr");
			pw.write("|^");
			pw.write("gmenoNo2Year");

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

					String ivstgGb_str = ivstgGb_Json.get("ivstgGb").toString();

					JSONArray ivstgs = (JSONArray) ivstgGb_Json.get("ivstgs");

					for (int r = 0; r < ivstgs.size(); r++) {

						JSONObject ivstg = (JSONObject) ivstgs.get(r);

						String ivstg_addr_str = "";
						String ivstg_ivstgSpotNm_str = "";
						String ivstg_xcnts_str = "";
						String ivstg_ydnts_str = "";

						if (ivstg.get("adres") != null) {
							ivstg_addr_str = ivstg.get("adres").toString();
						} else {
							ivstg_addr_str = "";
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
							String coVal = "";
							String ivstgEndde = "";
							String pm10Val = "";
							String pbVal = "";
							String no2Val = "";
							String o3Val = "";
							String so2Val = "";
							String ivstgBgnde = "";
							String ivstgOdr = "";
							String envrnGrad = "";
							String umenoNo224hr = "";
							String umenoCo1hr = "";
							String umenoNo21hr = "";
							String umenoSo21hr = "";
							String umenoSo2Year = "";
							String umenoPm10Year = "";
							String umenoCo8hr = "";
							String umenoPm1024hr = "";
							String umenoSo224hr = "";
							String umenoNo2Year = "";
							String gmenoNo21hr = "";
							String gmenoPm10Year = "";
							String gmenoNo224hr = "";
							String gmenoNo2Year = "";

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

								if (keyname.equals("coVal")) {
									coVal = odr.get(keyname).toString();
								}
								if (keyname.equals("ivstgEndde")) {
									ivstgEndde = odr.get(keyname).toString();
								}
								if (keyname.equals("pm10Val")) {
									pm10Val = odr.get(keyname).toString();
								}
								if (keyname.equals("pbVal")) {
									pbVal = odr.get(keyname).toString();
								}
								if (keyname.equals("no2Val")) {
									no2Val = odr.get(keyname).toString();
								}
								if (keyname.equals("o3Val")) {
									o3Val = odr.get(keyname).toString();
								}
								if (keyname.equals("so2Val")) {
									so2Val = odr.get(keyname).toString();
								}
								if (keyname.equals("ivstgBgnde")) {
									ivstgBgnde = odr.get(keyname).toString();
								}
								if (keyname.equals("ivstgOdr")) {
									ivstgOdr = odr.get(keyname).toString();
								}
								if (keyname.equals("envrnGrad")) {
									envrnGrad = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo224hr")) {
									umenoNo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoCo1hr")) {
									umenoCo1hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo21hr")) {
									umenoNo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo21hr")) {
									umenoSo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo2Year")) {
									umenoSo2Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm10Year")) {
									umenoPm10Year = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoCo8hr")) {
									umenoCo8hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoPm1024hr")) {
									umenoPm1024hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoSo224hr")) {
									umenoSo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("umenoNo2Year")) {
									umenoNo2Year = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo21hr")) {
									gmenoNo21hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoPm10Year")) {
									gmenoPm10Year = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo224hr")) {
									gmenoNo224hr = odr.get(keyname).toString();
								}
								if (keyname.equals("gmenoNo2Year")) {
									gmenoNo2Year = odr.get(keyname).toString();
								}
							}

							// step 4. 파일에 쓰기
							try {
								PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

								pw.write(mgtNo);
								pw.write("|^");
								pw.write(ivstgGb_str);
								pw.write("|^");
								pw.write(ivstg_ivstgSpotNm_str);
								pw.write("|^");
								pw.write(ivstg_addr_str);
								pw.write("|^");
								pw.write(ivstg_xcnts_str);
								pw.write("|^");
								pw.write(ivstg_ydnts_str);
								pw.write("|^");
								pw.write(coVal);
								pw.write("|^");
								pw.write(ivstgEndde);
								pw.write("|^");
								pw.write(pm10Val);
								pw.write("|^");
								pw.write(pbVal);
								pw.write("|^");
								pw.write(no2Val);
								pw.write("|^");
								pw.write(o3Val);
								pw.write("|^");
								pw.write(so2Val);
								pw.write("|^");
								pw.write(ivstgBgnde);
								pw.write("|^");
								pw.write(ivstgOdr);
								pw.write("|^");
								pw.write(envrnGrad);
								pw.write("|^");
								// 두번째 배열은 컬럼값이 다름
								pw.write(umenoNo224hr);
								pw.write("|^");
								pw.write(umenoCo1hr);
								pw.write("|^");
								pw.write(umenoNo21hr);
								pw.write("|^");
								pw.write(umenoSo21hr);
								pw.write("|^");
								pw.write(umenoSo2Year);
								pw.write("|^");
								pw.write(umenoPm10Year);
								pw.write("|^");
								pw.write(umenoCo8hr);
								pw.write("|^");
								pw.write(umenoPm1024hr);
								pw.write("|^");
								pw.write(umenoSo224hr);
								pw.write("|^");
								pw.write(umenoNo2Year);
								pw.write("|^");
								pw.write(gmenoNo21hr);
								pw.write("|^");
								pw.write(gmenoPm10Year);
								pw.write("|^");
								pw.write(gmenoNo224hr);
								pw.write("|^");
								pw.write(gmenoNo2Year);

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
