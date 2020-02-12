package foulsmellService;

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

public class FoulsmellService_getIvstg {

	final static Logger logger = Logger.getLogger(FoulsmellService_getIvstg.class);

	// 악취정보 서비스 -조사속성 조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		logger.info("firstLine start..");

		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("foulsmell_getIvstg_url");
		String service_key = JsonParser.getProperty("foulsmell_service_key");

		// step 1.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// step 2.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("FoulsmellService_getIvstg_" + strDate + ".dat");

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
			pw.write("cmpndBsmlVal"); // 복합악취
			pw.write("|^");
			pw.write("nh3Val"); // 암모니아
			pw.write("|^");
			pw.write("ch4sVal"); // 메틸메르캅탄
			pw.write("|^");
			pw.write("h2sVal"); // 황화수소
			pw.write("|^");
			pw.write("ch3sch3Val"); // 다이메틸설파이드
			pw.write("|^");
			pw.write("ch3ssch3Val"); // 다이메틸다이설파이드
			pw.write("|^");
			pw.write("ch33nVal"); // 트라이메틸아민
			pw.write("|^");
			pw.write("c2h4oVal"); // 아세트알데하이드
			pw.write("|^");
			pw.write("c8h8Val"); // 스타이렌
			pw.write("|^");
			pw.write("c3h6oVal"); // 프로피온알데하이드
			pw.write("|^");
			pw.write("ch3ch22choVal"); // 뷰틸알데하이드
			pw.write("|^");
			pw.write("ch3ch23choVal"); // n-발레르알데하이드
			pw.write("|^");
			pw.write("ch32chch2choVal"); // i-발레르알데하이드
			pw.write("|^");
			pw.write("c7h8Val"); // 톨루엔
			pw.write("|^");
			pw.write("c8h10Val"); // 자일렌
			pw.write("|^");
			pw.write("c4h8oVal"); // 메틸에틸케톤
			pw.write("|^");
			pw.write("c6h12oVal"); // 메틸아이소뷰틸케톤
			pw.write("|^");
			pw.write("c6h12o2Val"); // 뷰틸아세테이트
			pw.write("|^");
			pw.write("c2h5coohVal"); // 프로피온산
			pw.write("|^");
			pw.write("ch3ch22choohVal"); // n-뷰틸산
			pw.write("|^");
			pw.write("ch3ch23coohVal"); // n-발레르산
			pw.write("|^");
			pw.write("ch32chch2coohVal"); // i-발레르산
			pw.write("|^");
			pw.write("ch32chch2ohVal"); // i-뷰틸알코올
			pw.println();
			pw.flush();
			pw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int t = 0; t < businnessCodeList.size(); t++) {

			mgtNo = businnessCodeList.get(t).toString();
			
			logger.info("parsingMgtNo :" + mgtNo);

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
								String cmpndBsmlVal = ""; // 복합악취
								String nh3Val = ""; // 암모니아
								String ch4sVal = ""; // 메틸메르캅탄
								String h2sVal = ""; // 황화수소
								String ch3sch3Val = ""; // 다이메틸설파이드
								String ch3ssch3Val = ""; // 다이메틸다이설파이드
								String ch33nVal = ""; // 트라이메틸아민
								String c2h4oVal = ""; // 아세트알데하이드
								String c8h8Val = ""; // 스타이렌
								String c3h6oVal = ""; // 프로피온알데하이드
								String ch3ch22choVal = ""; // 뷰틸알데하이드
								String ch3ch23choVal = ""; // n-발레르알데하이드
								String ch32chch2choVal = ""; // i-발레르알데하이드
								String c7h8Val = ""; // 톨루엔
								String c8h10Val = ""; // 자일렌
								String c4h8oVal = ""; // 메틸에틸케톤
								String c6h12oVal = ""; // 메틸아이소뷰틸케톤
								String c6h12o2Val = ""; // 뷰틸아세테이트
								String c2h5coohVal = ""; // 프로피온산
								String ch3ch22choohVal = ""; // n-뷰틸산
								String ch3ch23coohVal = ""; // n-발레르산
								String ch32chch2coohVal = ""; // i-발레르산
								String ch32chch2ohVal = ""; // i-뷰틸알코올

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
									if (keyname.equals("cmpndBsmlVal")) {
										cmpndBsmlVal = odr.get(keyname).toString();
									}
									if (keyname.equals("nh3Val")) {
										nh3Val = odr.get(keyname).toString();
									}
									if (keyname.equals("ch4sVal")) {
										ch4sVal = odr.get(keyname).toString();
									}
									if (keyname.equals("h2sVal")) {
										h2sVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3sch3Val")) {
										ch3sch3Val = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3ssch3Val")) {
										ch3ssch3Val = odr.get(keyname).toString();
									}
									if (keyname.equals("ch33nVal")) {
										ch33nVal = odr.get(keyname).toString();
									}
									if (keyname.equals("c2h4oVal")) {
										c2h4oVal = odr.get(keyname).toString();
									}
									if (keyname.equals("c8h8Val")) {
										c8h8Val = odr.get(keyname).toString();
									}
									if (keyname.equals("c3h6oVal")) {
										c3h6oVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3ch22choVal")) {
										ch3ch22choVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3ch23choVal")) {
										ch3ch23choVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch32chch2choVal")) {
										ch32chch2choVal = odr.get(keyname).toString();
									}
									if (keyname.equals("c7h8Val")) {
										c7h8Val = odr.get(keyname).toString();
									}
									if (keyname.equals("c8h10Val")) {
										c8h10Val = odr.get(keyname).toString();
									}
									if (keyname.equals("c4h8oVal")) {
										c4h8oVal = odr.get(keyname).toString();
									}
									if (keyname.equals("c6h12oVal")) {
										c6h12oVal = odr.get(keyname).toString();
									}
									if (keyname.equals("c6h12o2Val")) {
										c6h12o2Val = odr.get(keyname).toString();
									}
									if (keyname.equals("c2h5coohVal")) {
										c2h5coohVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3ch22choohVal")) {
										ch3ch22choohVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch3ch23coohVal")) {
										ch3ch23coohVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch32chch2coohVal")) {
										ch32chch2coohVal = odr.get(keyname).toString();
									}
									if (keyname.equals("ch32chch2ohVal")) {
										ch32chch2ohVal = odr.get(keyname).toString();
									}

								}

								// step 4. 파일에 쓰기
								try {
									PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

									pw.write(mgtNo); // 사업코드
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
									pw.write(cmpndBsmlVal); // 복합악취
									pw.write("|^");
									pw.write(nh3Val); // 암모니아
									pw.write("|^");
									pw.write(ch4sVal); // 메틸메르캅탄
									pw.write("|^");
									pw.write(h2sVal); // 황화수소
									pw.write("|^");
									pw.write(ch3sch3Val); // 다이메틸설파이드
									pw.write("|^");
									pw.write(ch3ssch3Val); // 다이메틸다이설파이드
									pw.write("|^");
									pw.write(ch33nVal); // 트라이메틸아민
									pw.write("|^");
									pw.write(c2h4oVal); // 아세트알데하이드
									pw.write("|^");
									pw.write(c8h8Val); // 스타이렌
									pw.write("|^");
									pw.write(c3h6oVal); // 프로피온알데하이드
									pw.write("|^");
									pw.write(ch3ch22choVal); // 뷰틸알데하이드
									pw.write("|^");
									pw.write(ch3ch23choVal); // n-발레르알데하이드
									pw.write("|^");
									pw.write(ch32chch2choVal); // i-발레르알데하이드
									pw.write("|^");
									pw.write(c7h8Val); // 톨루엔
									pw.write("|^");
									pw.write(c8h10Val); // 자일렌
									pw.write("|^");
									pw.write(c4h8oVal); // 메틸에틸케톤
									pw.write("|^");
									pw.write(c6h12oVal); // 메틸아이소뷰틸케톤
									pw.write("|^");
									pw.write(c6h12o2Val); // 뷰틸아세테이트
									pw.write("|^");
									pw.write(c2h5coohVal); // 프로피온산
									pw.write("|^");
									pw.write(ch3ch22choohVal); // n-뷰틸산
									pw.write("|^");
									pw.write(ch3ch23coohVal); // n-발레르산
									pw.write("|^");
									pw.write(ch32chch2coohVal); // i-발레르산
									pw.write("|^");
									pw.write(ch32chch2ohVal); // i-뷰틸알코올
									pw.println();
									pw.flush();
									pw.close();

								} catch (IOException e) {
									e.printStackTrace();
								}

							}

						}

						// System.out.println("next");
						logger.info("mgtNo :" + mgtNo);
						

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
			File f = new File("FoulsmellService_getIvstg_" + strDate + ".dat");
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
