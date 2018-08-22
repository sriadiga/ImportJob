package com.krishagni.importcsv.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.krishagni.catissueplus.core.biospecimen.domain.factory.CprErrorCode;
import com.krishagni.catissueplus.core.biospecimen.events.CollectionProtocolRegistrationDetail;
import com.krishagni.catissueplus.core.biospecimen.events.ParticipantDetail;
import com.krishagni.catissueplus.core.biospecimen.events.PmiDetail;
import com.krishagni.catissueplus.core.biospecimen.services.CollectionProtocolRegistrationService;
import com.krishagni.catissueplus.core.common.errors.ErrorType;
import com.krishagni.catissueplus.core.common.errors.OpenSpecimenException;
import com.krishagni.catissueplus.core.common.events.RequestEvent;
import com.krishagni.catissueplus.core.common.events.ResponseEvent;
import com.krishagni.importcsv.datasource.DataSource;
import com.krishagni.importcsv.datasource.Impl.CsvFileDataSource;

public class CsvImporter {
	private final static String FILE_NAME = "/home/user/Music/participant.csv";
	
	private final static String DATE_FORMAT = "MM/dd/yyyy";
	
	private final static String FIRST_NAME = "First Name";
	
	private final static String LAST_NAME = "Last Name";
	
	private final static String PPID = "Treatment";
	
	private final static String MRN = "MRN";
	
	private final static String CP_SHORT_TITLE = "IRBNumber";
	
	private final static String VISIT_DATE = "Start Date";
	
	private final static String SITE_NAME = "Facility";
	
	private final static Log logger = LogFactory.getLog(CsvImporter.class);
	
	private OpenSpecimenException ose = new OpenSpecimenException(ErrorType.USER_ERROR);
	
	@Autowired
	private CollectionProtocolRegistrationService cprSvc;
	
	private DataSource dataSource;
	
	private int rowCount;
	
	public void importCsv() {
		dataSource = new CsvFileDataSource(FILE_NAME);
		rowCount = 0;
		
		try {
		    isHeaderRowValid(dataSource); 
		    while (dataSource.hasNext()) {
		    	Record record = dataSource.nextRecord();
		    	rowCount++;
		    	importParticipant(record);
		    }
		    ose.checkAndThrow();
		} catch (Exception e) {
		    logger.error("Error while parsing csv file : \n" + e.getMessage());
		} finally {
		    if (dataSource != null) {
		    	dataSource.close();
		    }
		}
	}
	
	private void importParticipant(Record record) throws ParseException {                
		CollectionProtocolRegistrationDetail cprDetail = new CollectionProtocolRegistrationDetail(); 
		cprDetail.setCpShortTitle(record.getValue(CP_SHORT_TITLE));
		cprDetail.setParticipant(new ParticipantDetail());
		cprDetail.setRegistrationDate(new SimpleDateFormat(DATE_FORMAT).parse(record.getValue(VISIT_DATE)));
		
		// Adding participant Details
		cprDetail.setPpid(record.getValue(PPID));
		cprDetail.getParticipant().setFirstName(record.getValue(FIRST_NAME));
		cprDetail.getParticipant().setLastName(record.getValue(LAST_NAME));
	
		// Setting PMI
		cprDetail.getParticipant().setPhiAccess(true);
		PmiDetail pmi = new PmiDetail();
		pmi.setMrn(record.getValue(MRN));
		pmi.setSiteName(record.getValue(SITE_NAME));
		cprDetail.getParticipant().setPmi(pmi);
		ResponseEvent<CollectionProtocolRegistrationDetail> resp = cprSvc.createRegistration(getRequest(cprDetail));
		
		if (resp.getError() != null) {
			ose.addError(CprErrorCode.NOT_FOUND, "Error at row " + String.valueOf(rowCount) + " " + resp.getError().getMessage() + "\n");
		}
	}
	
	private void isHeaderRowValid(DataSource dataSource) throws Exception {
		String[] csvHeaderRow = dataSource.getHeader();
		List<String> expectedHeader = new ArrayList<String>();
		
		expectedHeader.add(FIRST_NAME);
		expectedHeader.add(LAST_NAME);
		expectedHeader.add(PPID);
		expectedHeader.add(MRN);
		expectedHeader.add(CP_SHORT_TITLE);
		expectedHeader.add(VISIT_DATE);
		expectedHeader.add(SITE_NAME);
		
		for (String header : csvHeaderRow) {
			if (!expectedHeader.contains(header)) {
				throw new Exception("Headers of csv file does not match");
			}
		}
	}
	
	private <T> RequestEvent<T> getRequest(T payload) {
		return new RequestEvent<T>(payload);
	}
}