package airqualityService;

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

public class AirqualityService_getStackStdr {
	
	final static Logger logger = Logger.getLogger(AirqualityService_getStackStdr .class);
	
	
	// 대기질 정보 조회 - 연돌기준 속성 조회

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		logger.info("firstLine start..");
				String mgtNo = "";

				// step 0.open api url과 서비스 키.
				String service_url = JsonParser.getProperty("airquality_getStackStdr_url");
				String service_key = JsonParser.getProperty("airquality_service_key");
				
				// step 0.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
				List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

				// step 1.파일의 첫 행 작성
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
				Date thisDate = new Date();
				String strDate = format.format(thisDate);

				File file = new File("AirqualityService_getStackStdr_" + strDate + ".dat");
				
				try {
					PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
					pw.write("mgtNo"); //사업 코드
					pw.write("|^");
					pw.write("stackNm"); //연돌명
					pw.write("|^");
					pw.write("adres"); //주소
					pw.write("|^");
					pw.write("xcnts"); //X좌표 
					pw.write("|^");
					pw.write("ydnts"); //Y좌표
					pw.write("|^");
					pw.write("stackHg"); //연돌높이
					pw.write("|^");
					pw.write("stackDm"); //연돌직경
					pw.write("|^");
					pw.write("stackIndm"); //연돌내경
					pw.write("|^");
					pw.write("stackTp"); //배출가스온도
					pw.write("|^");
					pw.write("dgsnStdrPm10Val"); //미세먼지(10) 설계기준
					pw.write("|^");
					pw.write("dgsnStdrPm25Val"); //미세먼지(2.5) 설계기준
					pw.write("|^");
					pw.write("dgsnStdrNo2Val"); //이산화질소 설계기준
					pw.write("|^");
					pw.write("dgsnStdrSo2Val"); //아황산가스 설계기준
					pw.write("|^");
					pw.write("dgsnStdrCoVal"); //일산화탄소 설계기준
					pw.write("|^");
					pw.write("dscamtPm10Val"); //미세먼지(10) 배출량
					pw.write("|^");
					pw.write("dscamtPm25Val"); //미세먼지(2.5) 배출량
					pw.write("|^");
					pw.write("dscamtNo2Val"); //이산화질소 배출량
					pw.write("|^");
					pw.write("dscamtSo20Val"); //아황산가스 배출량
					pw.write("|^");
					pw.write("dscamtCoVal"); //일산화탄소 배출량
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
					
					//logger.info("json:::::"+json);

					// step 3.필요에 맞게 파싱
					
					try {
						
						JSONParser parser = new JSONParser();
						JSONObject obj = (JSONObject) parser.parse(json);
						JSONObject response = (JSONObject) obj.get("response");

						// response는 결과값 코드와 메시지를 가지는 header와 데이터 부분인 body로 구분
						JSONObject header = (JSONObject) response.get("header");
						JSONObject body = (JSONObject) response.get("body");
						
						String resultCode = header.get("resultCode").toString();
						
						//logger.info("mgtNo:::"+mgtNo+ " resultCode:::"+ resultCode);
						
						if (resultCode.equals("00")) {
							
							JSONArray stacks = (JSONArray) body.get("stacks");
						
							for (int i = 0; i < stacks.size(); i++) {
								
								JSONObject stack = (JSONObject) stacks.get(i);
								
								Set<String> key = stack.keySet();
								
								Iterator<String> iter = key.iterator();
								
								String stackNm = ""; //연돌명
								String adres = ""; //주소
								String xcnts = ""; //X좌표
								String ydnts = ""; //Y좌표
								String stackHg = ""; //연돌높이
								String stackDm = ""; //연돌직경
								String stackIndm = ""; //연돌내경
								String stackTp = ""; //배출가스온도
								String dgsnStdrPm10Val = ""; //미세먼지(10) 설계기준
								String dgsnStdrPm25Val = ""; //미세먼지(2.5) 설계기준
								String dgsnStdrNo2Val = ""; //이산화질소 설계기준
								String dgsnStdrSo2Val = ""; //아황산가스 설계기준
								String dgsnStdrCoVal = ""; //일산화탄소 설계기준
								String dscamtPm10Val = ""; //미세먼지(10) 배출량
								String dscamtPm25Val = ""; //미세먼지(2.5) 배출량
								String dscamtNo2Val = ""; //이산화질소 배출량
								String dscamtSo20Val = ""; //아황산가스 배출량
								String dscamtCoVal = ""; //일산화탄소 배출량
							
								while (iter.hasNext()) {
									String keyname = iter.next();
									
									if (keyname.equals("stackNm")) {
										stackNm = stack.get(keyname).toString();
									}
									if (keyname.equals("adres")) {
										adres = stack.get(keyname).toString();
									}
									if (keyname.equals("xcnts")) {
										xcnts = stack.get(keyname).toString();
									}
									if (keyname.equals("ydnts")) {
										ydnts = stack.get(keyname).toString();
									}
									if (keyname.equals("stackHg")) {
										stackHg = stack.get(keyname).toString();
									}
									if (keyname.equals("stackDm")) {
										stackDm = stack.get(keyname).toString();
									}
									if (keyname.equals("stackIndm")) {
										stackIndm = stack.get(keyname).toString();
									}
									if (keyname.equals("dgsnStdrPm10Val")) {
										dgsnStdrPm10Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dgsnStdrPm25Val")) {
										dgsnStdrPm25Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dgsnStdrNo2Val")) {
										dgsnStdrNo2Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dgsnStdrSo2Val")) {
										dgsnStdrSo2Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dgsnStdrCoVal")) {
										dgsnStdrCoVal = stack.get(keyname).toString();
									}
									if (keyname.equals("dscamtPm10Val")) {
										dscamtPm10Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dscamtPm25Val")) {
										dscamtPm25Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dscamtNo2Val")) {
										dscamtNo2Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dscamtSo20Val")) {
										dscamtSo20Val = stack.get(keyname).toString();
									}
									if (keyname.equals("dscamtCoVal")) {
										dscamtCoVal = stack.get(keyname).toString();
									}
									
								}
								
								// step 4. 파일에 쓰기
								try{
									PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
									
									pw.write(mgtNo); //사업코드
									pw.write("|^");
									pw.write(stackNm); //연돌명
									pw.write("|^");
									pw.write(adres); //주소
									pw.write("|^");
									pw.write(xcnts); //X좌표
									pw.write("|^");
									pw.write(ydnts); //Y좌표
									pw.write("|^");
									pw.write(stackHg); //연돌높이
									pw.write("|^");
									pw.write(stackDm); //연돌직경
									pw.write("|^");
									pw.write(stackIndm); //연돌내경
									pw.write("|^");
									pw.write(stackTp); //배출가스온도
									pw.write("|^");
									pw.write(dgsnStdrPm10Val); //미세먼지(10) 설계기준
									pw.write("|^");
									pw.write(dgsnStdrPm25Val); //미세먼지(2.5) 설계기준
									pw.write("|^");
									pw.write(dgsnStdrNo2Val); //이산화질소 설계기준
									pw.write("|^");
									pw.write(dgsnStdrSo2Val); //아황산가스 설계기준
									pw.write("|^");
									pw.write(dgsnStdrCoVal); //일산화탄소 설계기준
									pw.write("|^");
									pw.write(dscamtPm10Val); //미세먼지(10) 배출량
									pw.write("|^");
									pw.write(dscamtPm25Val); //미세먼지(2.5) 배출량
									pw.write("|^");
									pw.write(dscamtNo2Val); //이산화질소 배출량
									pw.write("|^");
									pw.write(dscamtSo20Val); //아황산가스 배출량
									pw.write("|^");
									pw.write(dscamtCoVal); //일산화탄소 배출량
									pw.println();
									pw.flush();
									pw.close();
									
								} catch (IOException e) {
									e.printStackTrace();
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

				//step 5. 대상 서버에 sftp로 보냄
				
				Session session = null;
				Channel channel = null;
				ChannelSftp channelSftp = null;
				
				logger.debug("preparing the host information for sftp.");
				
				
				try{
					JSch jsch = new JSch();
					session = jsch.getSession("agntuser", "172.29.129.11", 28);
					session.setPassword("Dpdlwjsxm1@");
					
					//host 연결
					java.util.Properties config = new java.util.Properties();
					config.put("StrictHostKeyChecking", "no");
					session.setConfig(config);
					session.connect();
					
					//sftp 채널 연결
					channel = session.openChannel("sftp");
					channel.connect();
					
					//파일 업로드 처리
					channelSftp = (ChannelSftp) channel;
					
					//channelSftp.cd("/data1/if_data/WEI"); //as-is, 연계서버에 떨어지는 위치
					channelSftp.cd("/data1/test"); //test
					File f = new File("AirqualityService_getStackStdr_" + strDate + ".dat");
					String fileName = f.getName();
					channelSftp.put(new FileInputStream(f), fileName);
					
				} catch(Exception e) {
					e.printStackTrace();
				} finally {
					//sftp 채널을 닫음
					channelSftp.exit();
					
					//채널 연결 해제
					channel.disconnect();
					
					//호스트 세션 종료
					session.disconnect();	
				}
				
				logger.info("parsing complete!");		
				
	}

}
