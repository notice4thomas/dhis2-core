package org.hisp.dhis.tracker.job;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.hisp.dhis.render.RenderService;
import org.hisp.dhis.tracker.AtomicMode;
import org.hisp.dhis.tracker.FlushMode;
import org.hisp.dhis.tracker.TrackerIdScheme;
import org.hisp.dhis.tracker.TrackerIdentifier;
import org.hisp.dhis.tracker.TrackerIdentifierParams;
import org.hisp.dhis.tracker.TrackerImportParams;
import org.hisp.dhis.tracker.TrackerImportStrategy;
import org.hisp.dhis.tracker.TrackerTest;
import org.hisp.dhis.tracker.ValidationMode;
import org.hisp.dhis.tracker.bundle.TrackerBundleMode;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Luciano Fiandesio
 */
public class TrackerImportParamsSerdeTest extends TrackerTest
{

    @Autowired
    private RenderService renderService;

    @Override
    protected void initTest()
    {

    }

    @Test
    public void testJsonSerialization()
        throws JSONException
    {
        TrackerIdentifierParams identifierParams = TrackerIdentifierParams.builder().idScheme( TrackerIdentifier.CODE )
            .programIdScheme(
                TrackerIdentifier.builder().idScheme( TrackerIdScheme.ATTRIBUTE ).value( "aaaa" ).build() )
            .build();

        TrackerImportParams trackerImportParams = TrackerImportParams.builder()
            .identifiers( identifierParams )
            .atomicMode( AtomicMode.OBJECT )
            .flushMode( FlushMode.OBJECT )
            .skipRuleEngine( true )
            .importStrategy( TrackerImportStrategy.DELETE )
            .validationMode( ValidationMode.SKIP )
            .build();

        String json = renderService.toJsonAsString( trackerImportParams );
        JSONAssert.assertEquals( "" +
            "{\"importMode\":\"COMMIT\"," +
            "\"identifiers\":{\"dataElementIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"orgUnitIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"programIdScheme\":{\"idScheme\":\"ATTRIBUTE\",\"value\":\"aaaa\"}," +
            "\"programStageIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"idScheme\":{\"idScheme\":\"CODE\"}," +
            "\"categoryOptionComboIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"categoryOptionIdScheme\":{\"idScheme\":\"UID\"}}," +
            "\"importStrategy\":\"DELETE\"," +
            "\"atomicMode\":\"OBJECT\"," +
            "\"flushMode\":\"OBJECT\"," +
            "\"validationMode\":\"SKIP\"," +
            "\"skipPatternValidation\":false," +
            "\"skipSideEffects\":false," +
            "\"skipRuleEngine\":true," +
            "\"trackedEntities\":[]," +
            "\"enrollments\":[]," +
            "\"events\":[]," +
            "\"relationships\":[]," +
            "\"username\":\"system-process\"}", json, JSONCompareMode.LENIENT );
    }

    @Test
    public void testJsonDeserialization()
        throws IOException
    {

        final String json = "" +
            "{\"importMode\":\"COMMIT\"," +
            "\"identifiers\":{\"dataElementIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"orgUnitIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"programIdScheme\":{\"idScheme\":\"ATTRIBUTE\",\"value\":\"aaaa\"}," +
            "\"programStageIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"idScheme\":{\"idScheme\":\"CODE\"}," +
            "\"categoryOptionComboIdScheme\":{\"idScheme\":\"UID\"}," +
            "\"categoryOptionIdScheme\":{\"idScheme\":\"UID\"}}," +
            "\"importStrategy\":\"DELETE\"," +
            "\"atomicMode\":\"OBJECT\"," +
            "\"flushMode\":\"OBJECT\"," +
            "\"validationMode\":\"SKIP\"," +
            "\"skipPatternValidation\":true," +
            "\"skipSideEffects\":true," +
            "\"skipRuleEngine\":true," +
            "\"trackedEntities\":[]," +
            "\"enrollments\":[]," +
            "\"events\":[]," +
            "\"relationships\":[]," +
            "\"username\":\"system-process\"}";

        final TrackerImportParams trackerImportParams = renderService.fromJson( json, TrackerImportParams.class );

        assertThat( trackerImportParams.getImportMode(), is( TrackerBundleMode.COMMIT ) );
        assertThat( trackerImportParams.getImportStrategy(), is( TrackerImportStrategy.DELETE ) );
        assertThat( trackerImportParams.getAtomicMode(), is( AtomicMode.OBJECT ) );
        assertThat( trackerImportParams.getFlushMode(), is( FlushMode.OBJECT ) );
        assertThat( trackerImportParams.getValidationMode(), is( ValidationMode.SKIP ) );
        assertThat( trackerImportParams.isSkipPatternValidation(), is( true ) );
        assertThat( trackerImportParams.isSkipSideEffects(), is( true ) );
        assertThat( trackerImportParams.isSkipRuleEngine(), is( true ) );
        assertThat( trackerImportParams.getUser(), is( nullValue() ) );

        TrackerIdentifierParams identifiers = trackerImportParams.getIdentifiers();
        assertThat( identifiers.getIdScheme(), is( TrackerIdentifier.CODE ) );
        assertThat( identifiers.getProgramIdScheme().getIdScheme(), is( TrackerIdScheme.ATTRIBUTE ) );
        assertThat( identifiers.getProgramIdScheme().getValue(), is( "aaaa" ) );

    }

}
