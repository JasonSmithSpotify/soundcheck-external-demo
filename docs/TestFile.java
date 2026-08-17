package com.spotify.yoshi;

import static com.spotify.grpc.metadata.RequestMetadataUtils.newClientHeaderInterceptor;
import static com.spotify.yoshi.Main.SERVICE_NAME;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Any;
import com.spotify.apollo.grpc.server.GrpcServerInterceptorsModule;
import com.spotify.apollo.hermes.client.HermesClientModule;
import com.spotify.browsecore.hollow.model.HGrid;
import com.spotify.browsecore.hollow.model.HItem;
import com.spotify.browsecore.hollow.model.HItemInGrid;
import com.spotify.browsecore.hollow.model.HSection;
import com.spotify.continuum.guice.ContinuumApolloModule;
import com.spotify.continuum.section.guice.ContinuumSectionProviderApolloModule;
import com.spotify.continuum.v1.ContentRequest;
import com.spotify.continuum.v1.GetAvailableSectionsRequest;
import com.spotify.continuum.v1.GetSectionContentsRequest;
import com.spotify.continuum.v1.RequestOptions;
import com.spotify.continuum.v1.SectionDescription;
import com.spotify.continuum.v1.SectionItem;
import com.spotify.continuum.v1.SectionService;
import com.spotify.continuum.v1.Visuals.Rendering;
import com.spotify.destinationtraits.v1.DestinationGenreMapping;
import com.spotify.destinationtraits.v1.DestinationGenreMappingServiceGrpc;
import com.spotify.destinationtraits.v1.ListResponse;
import com.spotify.feature_data.browse_feature_data.v1.BrowseSectionFeatureData;
import com.spotify.feature_data.browse_feature_data.v1.BrowseSectionKind;
import com.spotify.grpc.metadata.RequestMetadata;
import com.spotify.grpc.metadata.RequestMetadataUtils;
import com.spotify.junit5.extensions.Gcs;
import com.spotify.junit5.extensions.Nameless;
import com.spotify.junit5.extensions.servicehelper.v2.ServiceHelper;
import com.spotify.net.rpc.metadata.userinfo.UserInfo;
import com.spotify.popularity.v1.EntityPopularity;
import com.spotify.popularity.v1.OrderedPopularityResponse;
import com.spotify.popularity.v1.PopularityServiceGrpc;
import com.spotify.usercontext.v1.ApplyChildContentRestrictionsState;
import com.spotify.usercontext.v1.UserContext;
import com.spotify.yoshi.di.CarbonConsumerModule;
import com.spotify.yoshi.di.RemoteConfigClientModule;
import com.spotify.yoshi.di.TestAttributeResolverModule;
import com.spotify.yoshi.di.TestEventSenderModule;
import com.spotify.yoshi.di.TestHealthStatusManagerModule;
import com.spotify.yoshi.ranker.RankerType;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Nameless
@Gcs(
    project = ServiceIT.GCS_PROJECT,
    buckets = {ServiceIT.GCS_BUCKET})
@ServiceHelper
@TestMethodOrder(OrderAnnotation.class)
public class ServiceIT {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ServiceIT.class);

  static final String GCS_PROJECT = "my-test-project";
  static final String GCS_BUCKET = "my-test-bucket";
  public static final String GRID_ID_AUDIOBOOKS_GENRES = "5e04fedc42c1ae11";
  public static final String GRID_ID_NEW_GRID = "5e04fedc42c1ae12";
  public static final String ITEM_ID_HORROR_0 = "d8568d979535863a";
  public static final String ITEM_ID_HORROR_1 = "d8568d979535863b";
  public static final String ITEM_ID_HORROR_2 = "d8568d979535863c";
  public static final String ITEM_ID_HORROR_3 = "d8568d979535863d";
  public static final String ITEM_ID_HORROR_4 = "d8568d979535863e";
  public static final String SECTION_URI_TAGGED_FILTER = "spotify:section:0JQ5DAwD41j04sCgdpRvq3";
  public static final String URI_HORROR_0 = "spotify:page:0JQ5DAqbMKFK0EBNV8Wn6R";
  public static final String URI_HORROR_1 = "spotify:page:0JQ5DAqbMKFK0EBNV8Wn61";
  public static final String URI_HORROR_2 = "spotify:page:0JQ5DAqbMKFK0EBNV8Wn62";
  public static final String URI_HORROR_3 = "spotify:page:0JQ5DAqbMKFK0EBNV8Wn63";
  public static final String URI_HORROR_4 = "spotify:page:0JQ5DAqbMKFK0EBNV8Wn64";

  private SectionService.Client sectionServiceClient;
  private HealthGrpc.HealthBlockingStub healthServiceClient;
  private static TestHollowProducer testHollowProducer;

  @BeforeAll
  public static void beforeAll(final ServiceHelper.Setup setup, final Gcs.Bind gcsBind)
      throws IOException {

    gcsBind.enableResumableUploadsFromTest(); // <-- without this, it just hangs...

    final String gcsHost =
        "http://%s".formatted(gcsBind.getExternalHostAndPort().getHostAndPortString());

    testHollowProducer =
        new TestHollowProducer(GCS_BUCKET, GCS_PROJECT, gcsHost, gcsBind.storage());
    setup
        .create(new YoshiApplication())
        .conf("carbon.consumer.interval", 1)
        .conf("carbon.gcs.host", gcsHost)
        .conf("carbon.gcs.bucket", GCS_BUCKET)
        .conf("carbon.gcs.project", GCS_PROJECT)
        .conf("carbon.history.enable", false)
        .conf("carbon.explore.enable", false)
        .conf("use-noop-remote-config", true)
        .conf("contentControl.mode", "noop")
        .conf("bigtable.emulator.host", "localhost")
        .conf("bigtable.emulator.port", 0)
        .conf("i18n.locales.supported-scope", "SUPPORTED_LOCALES_SCOPE_SPOTIFY_MAIN_APPLICATION")
        .withModule(GrpcServerInterceptorsModule.create())
        .withModule(HermesClientModule.create())
        .withModule(TestAttributeResolverModule.create())
        .withModule(TestEventSenderModule.create())
        .withModule(TestHealthStatusManagerModule.create())
        .withModule(CarbonConsumerModule.create())
        .withModule(ContinuumApolloModule.create())
        .withModule(RemoteConfigClientModule.create())
        .withModule(
            ContinuumSectionProviderApolloModule.withBasePackage(Main.class.getPackageName()))
        .startTimeoutSeconds(60);
  }

  @BeforeEach
  void beforeEach(final ServiceHelper.Runtime runtime) {
    final var channel = runtime.getClientFactory().getGrpcChannel();

    sectionServiceClient =
        SectionService.client(channel)
            .withInterceptors(newClientHeaderInterceptor(Optional.of(SERVICE_NAME)))
            .withDeadlineAfter(60, TimeUnit.SECONDS);
    healthServiceClient =
        HealthGrpc.newBlockingStub(channel)
            .withInterceptors(newClientHeaderInterceptor(Optional.of(SERVICE_NAME)))
            .withDeadlineAfter(5, TimeUnit.SECONDS);
  }

  @Test
  @Order(1)
  void becomesServingAndReturnsGridContentOnlyAfterHollowIsReady() {
    final var healthRequest = HealthCheckRequest.newBuilder().setService(SERVICE_NAME).build();
    assertEquals(ServingStatus.NOT_SERVING, healthServiceClient.check(healthRequest).getStatus());

    testHollowProducer.runCycle(getDummyData());

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    ServingStatus.SERVING, healthServiceClient.check(healthRequest).getStatus()));

    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD41j04sCgdpRvq1")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils.attachRequestMetadata(
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              assertThat(sectionContents.getResponseList().size(), greaterThan(0));
              assertThat(
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .map(SectionItem::getUri)
                      .collect(toList()),
                  hasItem(URI_HORROR_0));
            });
  }

  @Test
  void shouldReturnAllAvailableSections() {
    testHollowProducer.runCycle(getDummyData());
    // invoke the GetAvailableSections rpc
    final var request = GetAvailableSectionsRequest.getDefaultInstance();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sections =
                  sectionServiceClient
                      .getAvailableSections(Context.current(), request)
                      .toCompletableFuture()
                      .get();
              assertEquals(5, sections.getSectionDescriptionList().size());
              assertTrue(
                  sections.getSectionDescriptionList().stream()
                      .map(SectionDescription::getName)
                      .toList()
                      .containsAll(
                          List.of(
                              "audiobook-genres",
                              "see-all-genres",
                              "Some name",
                              "ranked-section",
                              "tagged-filter-section")));
            });
  }

  @Test
  void shouldReturnSectionContents() {
    testHollowProducer.runCycle(getDummyData());
    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD41j04sCgdpRvq1")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils
                      .attachRequestMetadata( // couldn't get the @UserInfo anno to work
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              final var sectionItems =
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .toList();

              assertThat(sectionContents.getResponseList().size(), greaterThan(0));
              assertThat(
                  sectionItems.stream().map(SectionItem::getUri).collect(toList()),
                  hasItem(URI_HORROR_0));
            });
  }

  @Test
  void shouldReturnSectionContentsForSectionWithoutGridAndWithTargetLocation() {
    testHollowProducer.runCycle(getDummyData());
    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD41iRZZRVB8eoeB")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils
                      .attachRequestMetadata( // couldn't get the @UserInfo anno to work
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              final var sectionItems =
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .toList();

              assertThat(sectionContents.getResponseList().size(), greaterThan(0));
              assertTrue(sectionItems.isEmpty());
              assertEquals(
                  Any.pack(
                      BrowseSectionFeatureData.newBuilder()
                          .setKind(BrowseSectionKind.BROWSE_SECTION_EDITORIAL_PROMOTION)
                          .build()),
                  sectionContents.getResponseList().getFirst().getFeatureData());
              assertEquals(
                  "spotify:page:some-target",
                  sectionContents.getResponseList().getFirst().getTargetLocation());
            });
  }

  @Test
  void shouldReturnSectionContentsForSectionWithNameAndColorCode() {
    testHollowProducer.runCycle(getDummyData());
    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD41iRZZRVB8eoeC")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils
                      .attachRequestMetadata( // couldn't get the @UserInfo anno to work
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              final var sectionItems =
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .toList();

              assertThat(sectionContents.getResponseList().size(), greaterThan(0));
              assertTrue(sectionItems.isEmpty());
              assertEquals(
                  Any.pack(
                      BrowseSectionFeatureData.newBuilder()
                          .setKind(BrowseSectionKind.BROWSE_SECTION_CATEGORY_LIST)
                          .build()),
                  sectionContents.getResponseList().getFirst().getFeatureData());
              assertEquals(
                  "Some name",
                  sectionContents
                      .getResponseList()
                      .getFirst()
                      .getMetadataOrBuilder()
                      .getContentName());
              assertEquals(
                  "#444333",
                  sectionContents.getResponseList().getFirst().getVisuals().getColorCode());
            });
  }

  @Test
  void shouldReturnANewlyAddedSection() {
    testHollowProducer.runCycle(
        List.of(
            new HSection(
                "yt9341gtr5eadebe",
                "spotify:section:0JQ5DAwD51iRZZRVB8eoeD",
                "New Section",
                "Some name for new section",
                "#123456",
                null,
                Rendering.DEFAULT.name(),
                BrowseSectionKind.BROWSE_SECTION_CATEGORY_LIST.name(),
                null,
                null)));

    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD51iRZZRVB8eoeD")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils.attachRequestMetadata(
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();
              assertThat(sectionContents.getResponseList().size(), greaterThan(0));
              assertEquals(
                  Any.pack(
                      BrowseSectionFeatureData.newBuilder()
                          .setKind(BrowseSectionKind.BROWSE_SECTION_CATEGORY_LIST)
                          .build()),
                  sectionContents.getResponseList().getFirst().getFeatureData());
              assertEquals(
                  "Some name for new section",
                  sectionContents
                      .getResponseList()
                      .getFirst()
                      .getMetadataOrBuilder()
                      .getContentName());
            });
  }

  @Test
  void shouldReturnSectionContents_withRanking(ServiceHelper.Runtime runtime) {
    testHollowProducer.runCycle(getDummyData());

    runtime
        .grpcMocks()
        .whenUnary(PopularityServiceGrpc.getGetOrderedPopularityMethod())
        .thenReplyOk(
            OrderedPopularityResponse.newBuilder()
                .addEntities(
                    EntityPopularity.newBuilder()
                        .setEntityId("genre_id_1")
                        .setNormalizedPopularity(2.0)
                        .build())
                .addEntities(
                    EntityPopularity.newBuilder()
                        .setEntityId("genre_id_2")
                        .setNormalizedPopularity(1.0)
                        .build())
                .addEntities(
                    EntityPopularity.newBuilder()
                        .setEntityId("genre_id_3")
                        .setNormalizedPopularity(3.0)
                        .build())
                .build());

    runtime
        .grpcMocks()
        .whenUnary(DestinationGenreMappingServiceGrpc.getListMethod())
        .thenReplyOk(
            ListResponse.newBuilder()
                .addMapping(
                    DestinationGenreMapping.newBuilder()
                        .setGenreId("genre_id_1")
                        .setDestinationUri("spotify:genre:0JQ5DAqbMKFK0EBNV8Wn61")
                        .build())
                .addMapping(
                    DestinationGenreMapping.newBuilder()
                        .setGenreId("genre_id_2")
                        .setDestinationUri("spotify:genre:0JQ5DAqbMKFK0EBNV8Wn62")
                        .build())
                .addMapping(
                    DestinationGenreMapping.newBuilder()
                        .setGenreId("genre_id_3")
                        .setDestinationUri("spotify:genre:0JQ5DAqbMKFK0EBNV8Wn63")
                        .build())
                .build());

    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(RequestOptions.getDefaultInstance())
                    .setUri("spotify:section:0JQ5DAwD41j04sCgdpRvq2")
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils.attachRequestMetadata(
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              final var sectionItems =
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .toList();

              assertEquals(4, sectionItems.size());
              assertEquals(
                  List.of(URI_HORROR_4, URI_HORROR_3, URI_HORROR_1, URI_HORROR_2),
                  sectionItems.stream().map(SectionItem::getUri).collect(toList()));
            });
  }

  @Test
  void shouldNotReturnKidsRestrictedItemWhenKidsRestrictedRestrictionsApplied() {
    testHollowProducer.runCycle(getDummyData());
    // invoke the GetSectionContents rpc
    final var request =
        GetSectionContentsRequest.newBuilder()
            .addRequest(
                ContentRequest.newBuilder()
                    .setRequestOptions(
                        RequestOptions.newBuilder()
                            .setUserContext(
                                UserContext.newBuilder()
                                    .setApplyChildContentRestrictionsState(
                                        ApplyChildContentRestrictionsState
                                            .APPLY_CHILD_CONTENT_RESTRICTIONS_ENABLED)
                                    .build()))
                    .setUri(SECTION_URI_TAGGED_FILTER)
                    .build())
            .build();

    Awaitility.waitAtMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final var sectionContents =
                  RequestMetadataUtils.attachRequestMetadata(
                          sectionServiceClient,
                          getRequestMetadataFor("RO"),
                          Deadline.after(120, TimeUnit.SECONDS))
                      .getSectionContents(Context.current(), request)
                      .toCompletableFuture()
                      .get();

              final var sectionItems =
                  sectionContents.getResponseList().stream()
                      .flatMap(content -> content.getItemsList().stream())
                      .toList();

              // This uses the "new-grid" containing "horror" 1-4.
              // Nr 1 is kids restricted, 3 is mixed, 4 is audiobooks - those should all be removed,
              // leaving only Nr 2.
              assertThat(sectionItems.size(), equalTo(1));
              assertThat(
                  sectionItems.stream().map(SectionItem::getUri).collect(toList()),
                  hasItems(URI_HORROR_2));
            });
  }

  @Test
  void shouldNotReturnDsaIncompatibleItemWhenDsaIncompatibleRestrictionsApplied() {
    TestAttributeResolverModule.setDsaEnabled(true);
    try {
      testHollowProducer.runCycle(getDummyData());
      // invoke the GetSectionContents rpc
      final var request =
          GetSectionContentsRequest.newBuilder()
              .addRequest(ContentRequest.newBuilder().setUri(SECTION_URI_TAGGED_FILTER).build())
              .build();

      Awaitility.waitAtMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final var sectionContents =
                    RequestMetadataUtils.attachRequestMetadata(
                            sectionServiceClient,
                            getRequestMetadataFor("RO", "test-user"),
                            Deadline.after(120, TimeUnit.SECONDS))
                        .getSectionContents(Context.current(), request)
                        .toCompletableFuture()
                        .get();

                final var sectionItems =
                    sectionContents.getResponseList().stream()
                        .flatMap(content -> content.getItemsList().stream())
                        .toList();

                assertThat(sectionItems.size(), equalTo(3));
                assertThat(
                    sectionItems.stream().map(SectionItem::getUri).collect(toList()),
                    hasItems(URI_HORROR_1, URI_HORROR_3, URI_HORROR_4));
              });
    } finally {
      TestAttributeResolverModule.setDsaEnabled(false);
    }
  }

  private static RequestMetadata getRequestMetadataFor(final String country) {
    return RequestMetadata.newBuilder()
        .setUserInfo(UserInfo.newBuilder().setCountry(country).build())
        .build();
  }

  private static RequestMetadata getRequestMetadataFor(final String country, final String userId) {
    return RequestMetadata.newBuilder()
        .setUserInfo(UserInfo.newBuilder().setCountry(country).setUserId(userId).build())
        .build();
  }

  private List<Object> getDummyData() {
    return List.of(
        new HGrid(GRID_ID_AUDIOBOOKS_GENRES, "Audiobook Genres", "audiobook-genres"),
        new HGrid(GRID_ID_NEW_GRID, "Audiobook Genres New Grid", "audiobook-genres-new-grid"),
        new HItem(
            ITEM_ID_HORROR_0,
            URI_HORROR_0,
            "destinations::1pRgWC::Horror",
            null,
            Map.of(),
            List.of(),
            "MUSIC"),
        new HItem(
            ITEM_ID_HORROR_1,
            URI_HORROR_1,
            "destinations::1pRgWC::Horror1",
            null,
            Map.of(),
            List.of("KIDS_RESTRICTED"),
            "MUSIC"),
        new HItem(
            ITEM_ID_HORROR_2,
            URI_HORROR_2,
            "destinations::1pRgWC::Horror2",
            null,
            Map.of(),
            List.of("DSA_INCOMPATIBLE"),
            "MUSIC"),
        new HItem(
            ITEM_ID_HORROR_3,
            URI_HORROR_3,
            "destinations::1pRgWC::Horror3",
            null,
            Map.of(),
            List.of(),
            "MIXED"),
        new HItem(
            ITEM_ID_HORROR_4,
            URI_HORROR_4,
            "destinations::1pRgWC::Horror4",
            null,
            Map.of(),
            List.of(),
            "AUDIOBOOKS"),
        new HItemInGrid(
            "ce8741dba2ebae97", ITEM_ID_HORROR_0, GRID_ID_AUDIOBOOKS_GENRES, "RO", 1000, Map.of()),
        new HItemInGrid(
            "ce8741dba2ebae98", ITEM_ID_HORROR_1, GRID_ID_NEW_GRID, "RO", 2000, Map.of()),
        new HItemInGrid(
            "ce8741dba2ebae99", ITEM_ID_HORROR_2, GRID_ID_NEW_GRID, "RO", 3000, Map.of()),
        new HItemInGrid(
            "ce8741dba2ebae00", ITEM_ID_HORROR_3, GRID_ID_NEW_GRID, "RO", 4000, Map.of()),
        new HItemInGrid(
            "ce8741dba2ebae01", ITEM_ID_HORROR_4, GRID_ID_NEW_GRID, "RO", 5000, Map.of()),
        new HSection(
            "fe8741dba2ebae12",
            "spotify:section:0JQ5DAwD41j04sCgdpRvq1",
            "Audiobook Genres",
            "audiobook-genres",
            null,
            GRID_ID_AUDIOBOOKS_GENRES,
            Rendering.DEFAULT.name(),
            BrowseSectionKind.BROWSE_SECTION_CATEGORIES.name(),
            "",
            null),
        new HSection(
            "fe8741dba2ebae13",
            "spotify:section:0JQ5DAwD41j04sCgdpRvq2",
            "Ranked section",
            "ranked-section",
            null,
            GRID_ID_NEW_GRID,
            Rendering.DEFAULT.name(),
            BrowseSectionKind.BROWSE_SECTION_CATEGORIES.name(),
            "",
            List.of(RankerType.GENRE_POPULARITY.name())),
        new HSection(
            "fe8741dba2ebae14",
            SECTION_URI_TAGGED_FILTER,
            "Tagged filter section",
            "tagged-filter-section",
            null,
            GRID_ID_NEW_GRID,
            Rendering.DEFAULT.name(),
            BrowseSectionKind.BROWSE_SECTION_CATEGORIES.name(),
            "",
            List.of()),
        new HSection(
            "qw8741gfd2ebaebe",
            "spotify:section:0JQ5DAwD41iRZZRVB8eoeB",
            "See all Genres",
            "see-all-genres",
            null,
            null,
            Rendering.HEADER.name(),
            BrowseSectionKind.BROWSE_SECTION_EDITORIAL_PROMOTION.name(),
            "spotify:page:some-target",
            null),
        new HSection(
            "rt8741gfd2ebaebe",
            "spotify:section:0JQ5DAwD41iRZZRVB8eoeC",
            "Dont See all Genres",
            "Some name",
            "#444333",
            null,
            Rendering.DEFAULT.name(),
            BrowseSectionKind.BROWSE_SECTION_CATEGORY_LIST.name(),
            null,
            null));
  }
}
