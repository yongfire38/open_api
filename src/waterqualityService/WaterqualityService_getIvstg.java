package waterqualityService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import common.DBConnection;
import common.JsonParser;

public class WaterqualityService_getIvstg {

	final static Logger logger = Logger.getLogger(WaterqualityService_getIvstg.class);

	// 수질정보 서비스 - 개요 속성 조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		logger.info("firstLine start..");
		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("waterquality_getIvstg_url");
		String service_key = JsonParser.getProperty("waterquality_service_key");

		// step 0.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// step 1.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("WaterqualityService_getIvstg_" + strDate + ".dat");

		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

			pw.write("mgtNo"); // 사업 코드
			pw.write("|^");
			pw.write("ivstgGb"); // 조사구분
			pw.write("|^");
			pw.write("ivstgSpotNm"); // 조사지점명
			pw.write("|^");
			pw.write("adres"); // 주소
			pw.write("|^");
			pw.write("xcnts"); // X좌표
			pw.write("|^");
			pw.write("ydnts"); // Y좌표
			pw.write("|^");
			pw.write("ivstgOdr"); // 조사차수
			pw.write("|^");
			pw.write("ivstgBgnde"); // 조사시작일
			pw.write("|^");
			pw.write("ivstgEndde"); // 조사종료일
			pw.write("|^");
			pw.write("waterTp"); // 수온
			pw.write("|^");
			pw.write("phVal"); // 수소이온농도
			pw.write("|^");
			pw.write("doVal"); // 용존산소량
			pw.write("|^");
			pw.write("ssVal"); // 부유물질량
			pw.write("|^");
			pw.write("codVal"); // 화학적산소요구량
			pw.write("|^");
			pw.write("ftcVal"); // 총대장균군
			pw.write("|^");
			pw.write("fcVal"); // 분원성 대장균군
			pw.write("|^");
			pw.write("tnVal"); // 총질소
			pw.write("|^");
			pw.write("tpVal"); // 총인
			pw.write("|^");
			pw.write("bodVal"); // 생물화학적산소요구량
			pw.write("|^");
			pw.write("tocVal"); // 총유기탄소
			pw.write("|^");
			pw.write("cpaVal"); // 클로로필-a
			pw.write("|^");
			pw.write("fluxVal"); // 유량
			pw.write("|^");
			pw.write("ugnVal"); // 유기인
			pw.write("|^");
			pw.write("absVal"); // 음이온계면활성제
			pw.write("|^");
			pw.write("cdmVal"); // 카드뮴
			pw.write("|^");
			pw.write("pbVal"); // 납
			pw.write("|^");
			pw.write("pcbVal"); // 폴리클로리네이티드비페닐
			pw.write("|^");
			pw.write("cnVal"); // 시안
			pw.write("|^");
			pw.write("hgVal"); // 수은
			pw.write("|^");
			pw.write("cr6Val"); // 6가크롬
			pw.write("|^");
			pw.write("asVal"); // 비소
			pw.write("|^");
			pw.write("ctVal"); // 사염화탄소
			pw.write("|^");
			pw.write("deVal"); // 1,2-디클로로에탄
			pw.write("|^");
			pw.write("teVal"); // 테트라디크로로에탄
			pw.write("|^");
			pw.write("dmVal"); // 디클로로메탄
			pw.write("|^");
			pw.write("bzVal"); // 벤젠
			pw.write("|^");
			pw.write("cfVal"); // 클로로포름
			pw.write("|^");
			pw.write("dehfVal"); // 디에틸헥실프탈레이트
			pw.write("|^");
			pw.write("amVal"); // 안티몬
			pw.write("|^");
			pw.write("dxVal"); // 1,4-다이옥세인
			pw.write("|^");
			pw.write("faVal"); // 포름알데히드
			pw.write("|^");
			pw.write("hbVal"); // 핵사클로로벤젠
			pw.write("|^");
			pw.write("kmno4Cnsmpqy"); // KMnO4소비량
			pw.write("|^");
			pw.write("tssVal"); // 증발잔류물
			pw.write("|^");
			pw.write("no3Val"); // 질산성질소
			pw.write("|^");
			pw.write("nhVal"); // 암모니아성질소
			pw.write("|^");
			pw.write("soVal"); // 황산이온
			pw.write("|^");
			pw.write("fVal"); // 불소
			pw.write("|^");
			pw.write("feVal"); // 철
			pw.write("|^");
			pw.write("znVal"); // 아연
			pw.write("|^");
			pw.write("pnVal"); // 페놀
			pw.write("|^");
			pw.write("envrnGrad"); // 환경기준등급
			pw.write("|^");
			pw.write("ugrwtrPrpos"); // 지하수용도
			pw.write("|^");
			pw.write("ugrwtrDkps"); // 음용여부
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
				JSONObject header = (JSONObject) response.get("header");
				JSONObject body = (JSONObject) response.get("body");

				String resultCode = header.get("resultCode").toString();

				logger.info("mgtNo:::" + mgtNo + " resultCode:::" + resultCode);

				if (resultCode.equals("00")) {

					JSONArray ivstgGbs = (JSONArray) body.get("ivstgGbs");

					// 공통으로 갖는 사업 코드
					for (int i = 0; i < ivstgGbs.size(); i++) {

						JSONObject ivstgGb_Json = (JSONObject) ivstgGbs.get(i);

						String ivstgGb_str = ivstgGb_Json.get("ivstgGb").toString(); // 조사구분

						JSONArray ivstgs = (JSONArray) ivstgGb_Json.get("ivstgs");

						for (int r = 0; r < ivstgs.size(); r++) {

							JSONObject ivstg = (JSONObject) ivstgs.get(r);

							String ivstg_adres_str = ""; // 주소
							String ivstg_ivstgSpotNm_str = ""; // 조사지점명
							String ivstg_xcnts_str = ""; // X좌표
							String ivstg_ydnts_str = ""; // Y좌표

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

								String ivstgOdr = ""; // 조사차수
								String ivstgBgnde = ""; // 조사시작일
								String ivstgEndde = ""; // 조사종료일
								String waterTp = ""; // 수온
								String phVal = ""; // 수소이온농도
								String doVal = ""; // 용존산소량
								String ssVal = ""; // 부유물질량
								String codVal = ""; // 화학적산소요구량
								String ftcVal = ""; // 총대장균군
								String fcVal = ""; // 분원성 대장균군
								String tnVal = ""; // 총질소
								String tpVal = ""; // 총인
								String bodVal = ""; // 생물화학적산소요구량
								String tocVal = ""; // 총유기탄소
								String cpaVal = ""; // 클로로필-a
								String fluxVal = ""; // 유량
								String ugnVal = ""; // 유기인
								String absVal = ""; // 음이온계면활성제
								String cdmVal = ""; // 카드뮴
								String pbVal = ""; // 납
								String pcbVal = ""; // 폴리클로리네이티드비페닐
								String cnVal = ""; // 시안
								String hgVal = ""; // 수은
								String cr6Val = ""; // 6가크롬
								String asVal = ""; // 비소
								String ctVal = ""; // 사염화탄소
								String deVal = ""; // 1,2-디클로로에탄
								String teVal = ""; // 테트라디크로로에탄
								String dmVal = ""; // 디클로로메탄
								String bzVal = ""; // 벤젠
								String cfVal = ""; // 클로로포름
								String dehfVal = ""; // 디에틸헥실프탈레이트
								String amVal = ""; // 안티몬
								String dxVal = ""; // 1,4-다이옥세인
								String faVal = ""; // 포름알데히드
								String hbVal = ""; // 핵사클로로벤젠
								String kmno4Cnsmpqy = ""; // KMnO4소비량
								String tssVal = ""; // 증발잔류물
								String no3Val = ""; // 질산성질소
								String nhVal = ""; // 암모니아성질소
								String soVal = ""; // 황산이온
								String fVal = ""; // 불소
								String feVal = ""; // 철
								String znVal = ""; // 아연
								String pnVal = ""; // 페놀
								String envrnGrad = ""; // 환경기준등급
								String ugrwtrPrpos = ""; // 지하수용도
								String ugrwtrDkps = ""; // 음용여부

								while (iter.hasNext()) {
									String keyname = iter.next();

									if (keyname.equals("ivstgOdr")) {
										ivstgOdr = odr.get(keyname).toString();
									}
									if (keyname.equals("ivstgBgnde")) {
										ivstgBgnde = odr.get(keyname).toString();
									}
									if (keyname.equals("ivstgEndde")) {
										ivstgEndde = odr.get(keyname).toString();
									}
									if (keyname.equals("waterTp")) {
										waterTp = odr.get(keyname).toString();
									}
									if (keyname.equals("phVal")) {
										phVal = odr.get(keyname).toString();
									}
									if (keyname.equals("doVal")) {
										doVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ssVal")) {
										ssVal = odr.get(keyname).toString();
									}
									if (keyname.equals("codVal")) {
										codVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ftcVal")) {
										ftcVal = odr.get(keyname).toString();
									}
									if (keyname.equals("fcVal")) {
										fcVal = odr.get(keyname).toString();
									}
									if (keyname.equals("tnVal")) {
										tnVal = odr.get(keyname).toString();
									}
									if (keyname.equals("tpVal")) {
										tpVal = odr.get(keyname).toString();
									}
									if (keyname.equals("bodVal")) {
										bodVal = odr.get(keyname).toString();
									}
									if (keyname.equals("tocVal")) {
										tocVal = odr.get(keyname).toString();
									}
									if (keyname.equals("cpaVal")) {
										cpaVal = odr.get(keyname).toString();
									}
									if (keyname.equals("fluxVal")) {
										fluxVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ugnVal")) {
										ugnVal = odr.get(keyname).toString();
									}
									if (keyname.equals("absVal")) {
										absVal = odr.get(keyname).toString();
									}
									if (keyname.equals("cdmVal")) {
										cdmVal = odr.get(keyname).toString();
									}
									if (keyname.equals("pbVal")) {
										pbVal = odr.get(keyname).toString();
									}
									if (keyname.equals("pcbVal")) {
										pcbVal = odr.get(keyname).toString();
									}
									if (keyname.equals("cnVal")) {
										cnVal = odr.get(keyname).toString();
									}
									if (keyname.equals("hgVal")) {
										hgVal = odr.get(keyname).toString();
									}
									if (keyname.equals("cr6Val")) {
										cr6Val = odr.get(keyname).toString();
									}
									if (keyname.equals("asVal")) {
										asVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ctVal")) {
										ctVal = odr.get(keyname).toString();
									}
									if (keyname.equals("deVal")) {
										deVal = odr.get(keyname).toString();
									}
									if (keyname.equals("teVal")) {
										teVal = odr.get(keyname).toString();
									}
									if (keyname.equals("dmVal")) {
										dmVal = odr.get(keyname).toString();
									}
									if (keyname.equals("bzVal")) {
										bzVal = odr.get(keyname).toString();
									}
									if (keyname.equals("cfVal")) {
										cfVal = odr.get(keyname).toString();
									}
									if (keyname.equals("dehfVal")) {
										dehfVal = odr.get(keyname).toString();
									}
									if (keyname.equals("amVal")) {
										amVal = odr.get(keyname).toString();
									}
									if (keyname.equals("dxVal")) {
										dxVal = odr.get(keyname).toString();
									}
									if (keyname.equals("faVal")) {
										faVal = odr.get(keyname).toString();
									}
									if (keyname.equals("hbVal")) {
										hbVal = odr.get(keyname).toString();
									}
									if (keyname.equals("kmno4Cnsmpqy")) {
										kmno4Cnsmpqy = odr.get(keyname).toString();
									}
									if (keyname.equals("tssVal")) {
										tssVal = odr.get(keyname).toString();
									}
									if (keyname.equals("no3Val")) {
										no3Val = odr.get(keyname).toString();
									}
									if (keyname.equals("nhVal")) {
										nhVal = odr.get(keyname).toString();
									}
									if (keyname.equals("soVal")) {
										soVal = odr.get(keyname).toString();
									}
									if (keyname.equals("fVal")) {
										fVal = odr.get(keyname).toString();
									}
									if (keyname.equals("feVal")) {
										feVal = odr.get(keyname).toString();
									}
									if (keyname.equals("znVal")) {
										znVal = odr.get(keyname).toString();
									}
									if (keyname.equals("pnVal")) {
										pnVal = odr.get(keyname).toString();
									}
									if (keyname.equals("envrnGrad")) {
										envrnGrad = odr.get(keyname).toString();
									}
									if (keyname.equals("ugrwtrPrpos")) {
										ugrwtrPrpos = odr.get(keyname).toString();
									}
									if (keyname.equals("ugrwtrDkps")) {
										ugrwtrDkps = odr.get(keyname).toString();
									}
								}

								// step 4. 파일에 쓰기
								try {
									PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

									pw.write(mgtNo); // 사업 코드
									pw.write("|^");
									pw.write(ivstgGb_str); // 조사구분
									pw.write("|^");
									pw.write(ivstg_ivstgSpotNm_str); // 조사지점명
									pw.write("|^");
									pw.write(ivstg_adres_str); // 주소
									pw.write("|^");
									pw.write(ivstg_xcnts_str); // X좌표
									pw.write("|^");
									pw.write(ivstg_ydnts_str); // Y좌표
									pw.write("|^");
									pw.write(ivstgOdr); // 조사차수
									pw.write("|^");
									pw.write(ivstgBgnde); // 조사시작일
									pw.write("|^");
									pw.write(ivstgEndde); // 조사종료일
									pw.write("|^");
									pw.write(waterTp); // 수온
									pw.write("|^");
									pw.write(phVal); // 수소이온농도
									pw.write("|^");
									pw.write(doVal); // 용존산소량
									pw.write("|^");
									pw.write(ssVal); // 부유물질량
									pw.write("|^");
									pw.write(codVal); // 화학적산소요구량
									pw.write("|^");
									pw.write(ftcVal); // 총대장균군
									pw.write("|^");
									pw.write(fcVal); // 분원성 대장균군
									pw.write("|^");
									pw.write(tnVal); // 총질소
									pw.write("|^");
									pw.write(tpVal); // 총인
									pw.write("|^");
									pw.write(bodVal); // 생물화학적산소요구량
									pw.write("|^");
									pw.write(tocVal); // 총유기탄소
									pw.write("|^");
									pw.write(cpaVal); // 클로로필-a
									pw.write("|^");
									pw.write(fluxVal); // 유량
									pw.write("|^");
									pw.write(ugnVal); // 유기인
									pw.write("|^");
									pw.write(absVal); // 음이온계면활성제
									pw.write("|^");
									pw.write(cdmVal); // 카드뮴
									pw.write("|^");
									pw.write(pbVal); // 납
									pw.write("|^");
									pw.write(pcbVal); // 폴리클로리네이티드비페닐
									pw.write("|^");
									pw.write(cnVal); // 시안
									pw.write("|^");
									pw.write(hgVal); // 수은
									pw.write("|^");
									pw.write(cr6Val); // 6가크롬
									pw.write("|^");
									pw.write(asVal); // 비소
									pw.write("|^");
									pw.write(ctVal); // 사염화탄소
									pw.write("|^");
									pw.write(deVal); // 1,2-디클로로에탄
									pw.write("|^");
									pw.write(teVal); // 테트라디크로로에탄
									pw.write("|^");
									pw.write(dmVal); // 디클로로메탄
									pw.write("|^");
									pw.write(bzVal); // 벤젠
									pw.write("|^");
									pw.write(cfVal); // 클로로포름
									pw.write("|^");
									pw.write(dehfVal); // 디에틸헥실프탈레이트
									pw.write("|^");
									pw.write(amVal); // 안티몬
									pw.write("|^");
									pw.write(dxVal); // 1,4-다이옥세인
									pw.write("|^");
									pw.write(faVal); // 포름알데히드
									pw.write("|^");
									pw.write(hbVal); // 핵사클로로벤젠
									pw.write("|^");
									pw.write(kmno4Cnsmpqy); // KMnO4소비량
									pw.write("|^");
									pw.write(tssVal); // 증발잔류물
									pw.write("|^");
									pw.write(no3Val); // 질산성질소
									pw.write("|^");
									pw.write(nhVal); // 암모니아성질소
									pw.write("|^");
									pw.write(soVal); // 황산이온
									pw.write("|^");
									pw.write(fVal); // 불소
									pw.write("|^");
									pw.write(feVal); // 철
									pw.write("|^");
									pw.write(znVal); // 아연
									pw.write("|^");
									pw.write(pnVal); // 페놀
									pw.write("|^");
									pw.write(envrnGrad); // 환경기준등급
									pw.write("|^");
									pw.write(ugrwtrPrpos); // 지하수용도
									pw.write("|^");
									pw.write(ugrwtrDkps); // 음용여부
									pw.println();
									pw.flush();
									pw.close();

								} catch (IOException e) {
									e.printStackTrace();
								}

							}

						}

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
				logger.debug("mgtNo :" + mgtNo);
				// 패스하고 반복문 상단부터 재시작
				continue;
			}

			// 1초간 중지시킨다. (그냥 진행하면 응답 서버 에러 가능성)
			Thread.sleep(1000);

		}

		// step 5. 대상 서버에 sftp로 보냄

		Session session = null;
		Channel channel = null;
		ChannelSftp channelSftp = null;

		logger.debug("preparing the host information for sftp.");

		try {
			JSch jsch = new JSch();
			session = jsch.getSession("agntuser", "172.29.129.11", 28);
			session.setPassword("Dpdlwjsxm1@");

			// host 연결
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();

			// sftp 채널 연결
			channel = session.openChannel("sftp");
			channel.connect();

			// 파일 업로드 처리
			channelSftp = (ChannelSftp) channel;

			// channelSftp.cd("/data1/if_data/WEI"); //as-is, 연계서버에 떨어지는 위치
			channelSftp.cd("/data1/test"); // test
			File f = new File("WaterqualityService_getIvstg_" + strDate + ".dat");
			String fileName = f.getName();
			channelSftp.put(new FileInputStream(f), fileName);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// sftp 채널을 닫음
			channelSftp.exit();

			// 채널 연결 해제
			channel.disconnect();

			// 호스트 세션 종료
			session.disconnect();

		}

		logger.info("parsing complete!");

	}

}
