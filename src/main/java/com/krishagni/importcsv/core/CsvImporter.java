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
	//sri: eventually should not be hardcoded
	
	private final static String DATE_FORMAT = "MM/dd/yyyy";
	//sri: should not be hardcoded
	
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
			//sri: should send an email in case of error 
		} finally {
		    if (dataSource != null) {
		    	dataSource.close();
		    }
		}
	}
	
	private void importParticipant(Record record) throws ParseException {                
		CollectionProtocolRegistrationDetail cprDetail = new CollectionProtocolRegistrationDetail(); 
		cprDetail.setCpShortTitle(record.getValue("IRBNumber"));
		//sri: nothing should be harcoded in the code. this should be done from start. 
		//this kind of comments were given many times before
		//instead of writing code like this and then later cleaning up, write properly to start with.
		// move all hardcoding to resource file.
		
		cprDetail.setParticipant(new ParticipantDetail());
		cprDetail.setRegistrationDate(new SimpleDateFormat(DATE_FORMAT).parse(record.getValue("Start Date")));
		
		// Adding participant Details
		cprDetail.setPpid(record.getValue("Treatment"));
		cprDetail.getParticipant().setFirstName(record.getValue("First Name"));
		cprDetail.getParticipant().setLastName(record.getValue("Last Name"));
	
		// Setting PMI
		cprDetail.getParticipant().setPhiAccess(true);
		PmiDetail pmi = new PmiDetail();
		pmi.setMrn(record.getValue("MRN"));
		pmi.setSiteName("MSKCC Site");
		cprDetail.getParticipant().setPmi(pmi);
		ResponseEvent<CollectionProtocolRegistrationDetail> resp = cprSvc.createRegistration(getRequest(cprDetail));
		
		if (resp.getError() != null) {
			ose.addError(CprErrorCode.NOT_FOUND, "Error at row " + String.valueOf(rowCount) + " " + resp.getError().getMessage() + "\n");
			//sri this error msg is fine but not good enough
			//think how this will be used, imagine you are the IT guy managing this system.
			// and tell me what would make it easiest for you in case of errors
		}
	}
	
	private void isHeaderRowValid(DataSource dataSource) throws Exception {
		String[] csvHeaderRow = dataSource.getHeader();
		List<String> expectedHeader = new ArrayList<String>();
		
		expectedHeader.add("First Name");
		expectedHeader.add("Last Name");
		expectedHeader.add("Treatment");
		expectedHeader.add("MRN");
		expectedHeader.add("IRBNumber");
		expectedHeader.add("Start Date");
		//sri: this is why hard coding is bad. if you think like a "good programmer" in this method
		// you would have said "wait a minute, this is the 2nd time i am copy pasting the same header names
		// these are super basic programming concepts that i do not want to be giving comments at this level
		
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
