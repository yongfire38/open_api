package sntPbhService;

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

public class SntPbhService_getInfo {

	final static Logger logger = Logger.getLogger(SntPbhService_getInfo.class);

	// 위생공중보건정보조회 - 속성조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		logger.info("firstLine start..");

		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("sntPbh_getInfo_url");
		String service_key = JsonParser.getProperty("sntPbh_service_key");

		// step 1.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// step 2.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("SntPbh_getInfo_" + strDate + ".dat");

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
			pw.write("c8h8Val"); // 스타이렌
			pw.write("|^");
			pw.write("hclVal"); // 염화수소(HCL)
			pw.write("|^");
			pw.write("nh3Val"); // 암모니아
			pw.write("|^");
			pw.write("h2sVal"); // 황화수소
			pw.write("|^");
			pw.write("bzVal"); // 벤젠
			pw.write("|^");
			pw.write("hgVal"); // 수은(Hg)
			pw.write("|^");
			pw.write("niVal"); // 니켈(Ni)
			pw.write("|^");
			pw.write("cr6Val"); // 6가크롬(Cr6+)
			pw.write("|^");
			pw.write("cdVal"); // 카드뮴(Cd)
			pw.write("|^");
			pw.write("asVal"); // 비소(As)
			pw.write("|^");
			pw.write("hchoVal"); // 포름알데히드
			pw.write("|^");
			pw.write("vcVal"); // 염화비닐
			pw.write("|^");
			pw.write("hcnVal"); // 시안화수소
			pw.write("|^");
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

				logger.info("mgtNo:::"+mgtNo+ " resultCode:::"+ resultCode);

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
								String c8h8Val = ""; // 스타이렌
								String hclVal = ""; // 염화수소(HCL)
								String nh3Val = ""; // 암모니아
								String h2sVal = ""; // 황화수소
								String bzVal = ""; // 벤젠
								String hgVal = ""; // 수은(Hg)
								String niVal = ""; // 니켈(Ni)
								String cr6Val = ""; // 6가크롬(Cr6+)
								String cdVal = ""; // 카드뮴(Cd)
								String asVal = ""; // 비소(As)
								String hchoVal = ""; // 포름알데히드
								String vcVal = ""; // 염화비닐
								String hcnVal = ""; // 시안화수소

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
									if (keyname.equals("c8h8Val")) {
										c8h8Val = odr.get(keyname).toString();
									}
									if (keyname.equals("hclVal")) {
										hclVal = odr.get(keyname).toString();
									}
									if (keyname.equals("nh3Val")) {
										nh3Val = odr.get(keyname).toString();
									}
									if (keyname.equals("h2sVal")) {
										h2sVal = odr.get(keyname).toString();
									}
									if (keyname.equals("bzVal")) {
										bzVal = odr.get(keyname).toString();
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
									if (keyname.equals("hcnVal")) {
										hcnVal = odr.get(keyname).toString();
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
									pw.write(c8h8Val); // 스타이렌
									pw.write("|^");
									pw.write(hclVal); // 염화수소(HCL)
									pw.write("|^");
									pw.write(nh3Val); // 암모니아
									pw.write("|^");
									pw.write(h2sVal); // 황화수소
									pw.write("|^");
									pw.write(bzVal); // 벤젠
									pw.write("|^");
									pw.write(hgVal); // 수은(Hg)
									pw.write("|^");
									pw.write(niVal); // 니켈(Ni)
									pw.write("|^");
									pw.write(cr6Val); // 6가크롬(Cr6+)
									pw.write("|^");
									pw.write(cdVal); // 카드뮴(Cd)
									pw.write("|^");
									pw.write(asVal); // 비소(As)
									pw.write("|^");
									pw.write(hchoVal); // 포름알데히드
									pw.write("|^");
									pw.write(vcVal); // 염화비닐
									pw.write("|^");
									pw.write(hcnVal); // 시안화수소
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


		// step 6. 대상 서버에 sftp로 보냄

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
			File f = new File("SntPbh_getInfo_" + strDate + ".dat");
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
