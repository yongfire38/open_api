package airqualityService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class OpenApi_oneDepth {
	
	final static Logger logger = Logger.getLogger(OpenApi_oneDepth.class);

	// 오픈 api - 1차원 형태의 데이터를 파싱하여 파일로 작성
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		logger.info("firstLine start..");

		String mgtNo = "";

		// step 0.open api url과 서비스 키.
		String service_url = JsonParser.getProperty("airquality_getPredict_url");
		String service_key = JsonParser.getProperty("airquality_service_key");

		// step 1.사업코드 값을 DB에서 읽어 와 리스트에 저장 후 리턴.
		List<String> businnessCodeList = DBConnection.getBusinnessCodeList();

		// 파일 최상단에 작성할 키 값을 중복 없이 담기 위해 생성 (데이터 요청할 때마다 나오는 컬럼 개수가 일정하지 않아 처리)
		LinkedHashSet<String> keyHashSet = new LinkedHashSet<String>();

		// 요청 파라미터(사업 코드 개수)만큼 순회 - 테스트 시에는 반복 회수 조절 필요. 1300여개나 됨..
		for (int t = 0; t < businnessCodeList.size(); t++) {

			mgtNo = businnessCodeList.get(t).toString();

			String json = "";

			json = JsonParser.parseJson(service_url, service_key, mgtNo);

			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(json);
			JSONObject response = (JSONObject) obj.get("response");

			// response는 결과값 코드와 메시지를 가지는 header와 데이터 부분인 body로 구분
			JSONObject header = (JSONObject) response.get("header");
			JSONObject body = (JSONObject) response.get("body");

			String resultCode = header.get("resultCode").toString();

			//전체를 파싱하면서 파일 최상단에 작성할 키 값 수집
			if (resultCode.equals("00")) {

				for (int i = 0; i < body.size(); i++) {
					Set<String> key = body.keySet();

					Iterator<String> iter = key.iterator();

					while (iter.hasNext()) {
						String keyname = iter.next();
						keyHashSet.add(keyname);					
					}
				}
				
			}

			Thread.sleep(1000);
		}

		// step 2.수집된 키 값을 파일의 첫 행에 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);

		File file = new File("oneDepth_result_" + strDate + ".dat");

		Iterator<String> keyIter = keyHashSet.iterator();
		
		//본문 파싱 구문으로 들어가면 키값 돌면서 비교해주기 위해 한번이라도 나온 컬럼들을 리스트에 저장(전체 컬럼 리스트)
		List<String> colList = new ArrayList<String>();

		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));

			while (keyIter.hasNext()) {
				String key = keyIter.next();
				pw.write(key);
				colList.add(key);
				pw.write("|^");
			}

			pw.println();
			pw.flush();
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		logger.info("firstLine complete!!");
		
		logger.info("mainContents start..");
		

		Map<String, String> dataMap = new LinkedHashMap<String, String>();

		// step 3.오픈 api를 이용해 json 형태로 받아 옴 (요청 파라미터 횟수만큼 반복) - 테스트 시에는 반복 회수 조절 필요. 1300여개나 됨..

		for (int t = 0; t < businnessCodeList.size(); t++) {

			mgtNo = businnessCodeList.get(t).toString();

			String json = "";

			json = JsonParser.parseJson(service_url, service_key, mgtNo);
			
			// step 4.필요에 맞게 파싱
			
			try {
				
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(json);
				JSONObject response = (JSONObject) obj.get("response");

				// response는 결과값 코드와 메시지를 가지는 header와 데이터 부분인 body로 구분
				JSONObject header = (JSONObject) response.get("header");
				JSONObject body = (JSONObject) response.get("body");
				
				String resultCode = header.get("resultCode").toString();				
				
				if (resultCode.equals("00")) {				
					
					for (int i = 0; i < body.size(); i++) {
						Set<String> key = body.keySet();

						Iterator<String> iter = key.iterator();

						//logger.info("mgtNo : " + mgtNo +" content : " + body);
						
						for(int r = 0; r < colList.size(); r++){

							dataMap.put(colList.get(r), "");
			
						}
						
						while (iter.hasNext()) {
							String keyname = iter.next();
									
							for(int y = 0; y < colList.size(); y++){
								if(dataMap.containsKey(keyname)){							
									dataMap.put(keyname, body.get(keyname).toString());	
								}else{
									dataMap.put("", "");
								}
							}
		
							/*logger.info("key : " + keyname +" type : " + body.get(keyname).getClass());
							
							logger.info("value : " + body.get(keyname).toString());*/
	
						}
						
					}
					
					// step 5. 파일에 쓰기
					PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
					Iterator<String> mapIter = dataMap.keySet().iterator();

					while(mapIter.hasNext()){

						String dataMapkey = mapIter.next();
						String value = dataMap.get( dataMapkey );

						//logger.info("dataMap :::::::: "+dataMapkey+" : "+value);
						
						pw.write(value);
						pw.write("|^");

					}

					pw.println();
					

					pw.flush();
					pw.close();

				}
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}

			// 1초간 중지시킨다. (그냥 진행하면 응답 서버 에러 가능성)
			Thread.sleep(1000);

		}

		// step 6. 각 행의 마지막 구분자는 삭제
		
		FileEdit ie = new FileEdit();
		
		ie.editStringInFile(file);
		
		logger.info("complete!!");

	}
}
