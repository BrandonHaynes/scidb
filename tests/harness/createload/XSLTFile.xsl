<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
	<html>
		<body>
			<h2>SciDB Test Harness Result</h2>
			<h2></h2>
			<table width="750" border="1">
				<tr bgcolor="#aed8e8">
				<th colspan="2">Parameters passed to Test Harness</th>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>SciDB Server </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/scidbServer"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>SciDB Port </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/scidbPort"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>SciDB Root Dir </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/rootDir"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Name of the file containing disabled test ids </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/skipTestfname"/></td>
				</tr>

				<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag &gt; 0">
				<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag &lt; 5">
					<tr bgcolor="#e6f2f9">
					<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag = 1">
						<td>Include test case IDs that match the regex </td>
					</xsl:if>
					<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag = 2">
						<td>Exclude test case IDs that match the regex </td>
					</xsl:if>
					<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag = 3">
						<td>Include test case NAMEs that match the regex </td>
					</xsl:if>
					<xsl:if test="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexFlag = 4">
						<td>Exclude test case NAMEs that match the regex </td>
					</xsl:if>

					<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/regexExpr"/></td>
					</tr>
				</xsl:if>
				</xsl:if>

				<tr bgcolor="#e6f2f9">
				<td>Log Directory </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/logDir"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Report File Name </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/reportFilename"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Number of test cases to be run in Parallel </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/parallelTestCases"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>DebugLevel </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/debugLevel"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Record </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/record"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>KeepPreviousRun </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/keepPreviousRun"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>TerminateOnFailure </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/SciDBHarnessEnv/terminateOnFailure"/></td>
				</tr>
			</table>
			<br />
			<table width="3000" border="1">
				<tr bgcolor="#aed8e8">
					<th colspan="14">Failed Test Results</th>
				</tr>
				<tr bgcolor="#e6f2f9">
					<td>ID</td>
					<td>Result</td>
					<td>Log File</td>
					<td>Diff File</td>
					<!--td>Name</td-->
					<!--td>Description</td-->
					<td>Start Time</td>
					<td>End Time</td>
					<td>Test Execution Time (sec)</td>
					<!--td>Total Time (sec)</td-->
					<td>Testcase File</td>
					<td>Expected Output File</td>
					<td>Actual Output File</td>
					<td>Failure Reason</td>
				</tr>

				<xsl:for-each select="boost_serialization/SciDBTestReport/TestResults/IndividualTestResult">
				<xsl:sort select="TestcaseFailureReason" order="descending"/>
				<xsl:if test="TestcaseResult = 'FAIL'">
				<tr>
				<td><xsl:value-of select="TestID"/></td>
				<xsl:choose>
				<xsl:when test="TestcaseFailureReason = 'Expected output and Actual Output differ. Check .diff file.'">
				<td bgcolor="Yellow"><xsl:value-of select="TestcaseResult"/></td>
				</xsl:when>
				<xsl:when test="TestcaseResult = 'FAIL'">
				<td bgcolor="Red"><xsl:value-of select="TestcaseResult"/></td>
				</xsl:when>
				<xsl:otherwise>
				<td><xsl:value-of select="TestcaseResult"/></td>
				</xsl:otherwise>
				</xsl:choose>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseLogFile"/></xsl:attribute><xsl:value-of select="TestcaseLogFile"/></a></td>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseDiffFile"/></xsl:attribute><xsl:value-of select="TestcaseDiffFile"/></a></td>
				<!--td><xsl:value-of select="TestName"/></td-->
				<!--td><xsl:value-of select="TestDescription"/></td-->
				<td><xsl:value-of select="TestStartTime"/></td>
				<td><xsl:value-of select="TestEndTime"/></td>
				<td><xsl:value-of select="TestTotalExeTime"/></td>
				<!--td><xsl:value-of select="TotalTime"/></td-->
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseFile"/></xsl:attribute><xsl:value-of select="TestcaseFile"/></a></td>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseExpectedResultFile"/></xsl:attribute><xsl:value-of select="TestcaseExpectedResultFile"/></a></td>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseActualResultFile"/></xsl:attribute><xsl:value-of select="TestcaseActualResultFile"/></a></td>

				<td><xsl:value-of select="TestcaseFailureReason"/></td>
				</tr>
				</xsl:if>
				</xsl:for-each>
				</table>
			<br />
			<table width="3000" border="1">
				<tr bgcolor="#aed8e8">
					<th colspan="14">Passed Test Results</th>
				</tr>
				<tr bgcolor="#e6f2f9">
					<td>ID</td>
					<td>Result</td>
					<!--td>Name</td-->
					<!--td>Description</td-->
					<td>Start Time</td>
					<td>End Time</td>
					<td>Test Execution Time (sec)</td>
					<!--td>Total Time (sec)</td-->
					<td>Testcase File</td>
					<td>Expected Output File</td>
					<td>Actual Output File</td>
					<td>Log File</td>
				</tr>

				<xsl:for-each select="boost_serialization/SciDBTestReport/TestResults/IndividualTestResult">
				<xsl:if test="TestcaseResult = 'PASS' or TestcaseResult = 'RECORDED'">
				<tr>
				<td><xsl:value-of select="TestID"/></td>
				<xsl:choose>
				<xsl:when test="TestcaseResult = 'FAIL'">
				<td bgcolor="Red"><xsl:value-of select="TestcaseResult"/></td>
				</xsl:when>
				<xsl:otherwise>
				<td><xsl:value-of select="TestcaseResult"/></td>
				</xsl:otherwise>
				</xsl:choose>
				<!--td><xsl:value-of select="TestName"/></td-->
				<!--td><xsl:value-of select="TestDescription"/></td-->
				<td><xsl:value-of select="TestStartTime"/></td>
				<td><xsl:value-of select="TestEndTime"/></td>
				<td><xsl:value-of select="TestTotalExeTime"/></td>
				<!--td><xsl:value-of select="TotalTime"/></td-->
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseFile"/></xsl:attribute><xsl:value-of select="TestcaseFile"/></a></td>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseExpectedResultFile"/></xsl:attribute><xsl:value-of select="TestcaseExpectedResultFile"/></a></td>
				<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseActualResultFile"/></xsl:attribute><xsl:value-of select="TestcaseActualResultFile"/></a></td>
								<td><a><xsl:attribute name="href"><xsl:value-of select="TestcaseLogFile"/></xsl:attribute><xsl:value-of select="TestcaseLogFile"/></a></td>
				</tr>
				</xsl:if>
				</xsl:for-each>
				</table>

				<!-- *** Intermediate Stat table ***  -->
				<xsl:if test="boost_serialization/SciDBTestReport/IntermediateStats/TotalTestsPassed != ''">
				<br />
				<table width="750" border="1">
				<tr bgcolor="#aed8e8">
				<th colspan="2">Test Statistics</th>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Tests Passed so far </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/IntermediateStats/TotalTestsPassed"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Tests Failed so far </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/IntermediateStats/TotalTestsFailed"/></td>
				</tr>
				</table>
				</xsl:if>

				<!-- *** Final Stat table ***  -->
				<xsl:if test="boost_serialization/SciDBTestReport/FinalStats/TotalTestsPassed != ''">
				<br />
				<table width="750" border="1">
				<tr bgcolor="#aed8e8">
				<th colspan="2">Final Test Statistics</th>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Total Number of Tests scheduled for run </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/FinalStats/TotalTestCases"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Total Number of Tests Passed </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/FinalStats/TotalTestsPassed"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Total Number of Tests Failed </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/FinalStats/TotalTestsFailed"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Total Number of Tests Skipped </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/FinalStats/TotalTestsSkipped"/></td>
				</tr>

				<tr bgcolor="#e6f2f9">
				<td>Total Number of Test Suites Skipped </td>
				<td><xsl:value-of select="boost_serialization/SciDBTestReport/FinalStats/TotalSuitesSkipped"/></td>
				</tr>

				</table>
				</xsl:if>
		</body>
	</html>
</xsl:template>
</xsl:stylesheet>
