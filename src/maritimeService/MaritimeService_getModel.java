package maritimeService;

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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import common.DBConnection;
import common.JsonParser;

public class MaritimeService_getModel {

	final static Logger logger = Logger.getLogger(MaritimeService_getModel.class);

	// 해양환경정보 조회 - 적용모델속서조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		logger.info("firstLine start..");

		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("maritime_getModel_url");
		String service_key = JsonParser.getProperty("maritime_service_key");

		// step 1.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// step 2.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("MaritimeService_getModel_" + strDate + ".dat");

		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			pw.write("mgtNo"); // 사업 코드
			pw.write("|^");
			pw.write("seawaterFlowModel"); // 해수유동 적용모델
			pw.write("|^");
			pw.write("bfeSusDffAr"); // 부유사확산 면적(1㎎/ℓ) 저감대책 전
			pw.write("|^");
			pw.write("aftSusDffAr"); // 부유사확산 면적(1㎎/ℓ) 저감대책 후
			pw.write("|^");
			pw.write("bfeSusDffLt"); // 부유사확산 거리(1㎎/ℓ) 저감대책 전
			pw.write("|^");
			pw.write("aftSusDffLt"); // 부유사확산 거리(1㎎/ℓ) 저감대책 후
			pw.write("|^");
			pw.write("rm"); // 비고
			pw.println();
			pw.flush();
			pw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

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

				if (resultCode.equals("00")) {

					Set<String> key = body.keySet();

					Iterator<String> iter = key.iterator();

					String seawaterFlowModel = ""; // 해수유동 적용모델
					String bfeSusDffAr = ""; // 부유사확산 면적(1㎎/ℓ) 저감대책 전
					String aftSusDffAr = ""; // 부유사확산 면적(1㎎/ℓ) 저감대책 후
					String bfeSusDffLt = ""; // 부유사확산 거리(1㎎/ℓ) 저감대책 전
					String aftSusDffLt = ""; // 부유사확산 거리(1㎎/ℓ) 저감대책 후
					String rm = ""; // 비고

					while (iter.hasNext()) {
						String keyname = iter.next();

						if (keyname.equals("seawaterFlowModel")) {
							seawaterFlowModel = body.get(keyname).toString();
						}
						if (keyname.equals("bfeSusDffAr")) {
							bfeSusDffAr = body.get(keyname).toString();
						}
						if (keyname.equals("aftSusDffAr")) {
							aftSusDffAr = body.get(keyname).toString();
						}
						if (keyname.equals("bfeSusDffLt")) {
							bfeSusDffLt = body.get(keyname).toString();
						}
						if (keyname.equals("aftSusDffLt")) {
							aftSusDffLt = body.get(keyname).toString();
						}
						if (keyname.equals("rm")) {
							rm = body.get(keyname).toString();
						}

					}

					// step 4. 파일에 쓰기
					try {
						PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

						pw.write(mgtNo); // 사업 코드
						pw.write("|^");
						pw.write(seawaterFlowModel); // 해수유동 적용모델
						pw.write("|^");
						pw.write(bfeSusDffAr); // 부유사확산 면적(1㎎/ℓ) 저감대책 전
						pw.write("|^");
						pw.write(aftSusDffAr); // 부유사확산 면적(1㎎/ℓ) 저감대책 후
						pw.write("|^");
						pw.write(bfeSusDffLt); // 부유사확산 거리(1㎎/ℓ) 저감대책 전
						pw.write("|^");
						pw.write(aftSusDffLt); // 부유사확산 거리(1㎎/ℓ) 저감대책 후
						pw.write("|^");
						pw.write(rm); // 비고
						pw.println();
						pw.flush();
						pw.close();

					} catch (IOException e) {
						e.printStackTrace();
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
			File f = new File("MaritimeService_getModel_" + strDate + ".dat");
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
