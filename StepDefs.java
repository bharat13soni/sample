package com.epam.monity.exgester.bdd;

import static com.epam.monity.common.data.repository.common.model.SubmissionStatus.SUPPRESS;
import static com.epam.monity.exgester.util.EnumDescriptorUtil.isFeeTrade;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.core.ConfigurationWhen.path;
import static net.javacrumbs.jsonunit.core.ConfigurationWhen.then;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static test.utils.TestMessageProducer.getSampleLineageMessage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.epam.monity.common.data.repository.request.SubmissionSearchRequest;
import com.epam.monity.exgester.rest.upi.MockUpiResolverClientConfig;
import com.epam.streaming.model.determinator.in.TimeLinessTimeStamp;
import com.epam.streaming.model.internal.enumeration.ClearingStatus;
import com.epam.streaming.model.internal.enumeration.ClearingExceptionType;
import com.epam.streaming.model.internal.enumeration.ConfirmationMethod;
import com.epam.streaming.model.internal.enumeration.LegType;
import com.epam.streaming.model.internal.enumeration.OptionStyle;
import com.epam.streaming.model.internal.enumeration.OptionType;
import com.epam.streaming.model.internal.enumeration.PriceUnit;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import com.ihsmarkit.ir.alert.client.AlertSender;
import com.ihsmarkit.ir.alert.message.NewAlert;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.assertj.core.api.Assertions;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Change;
import org.javers.core.diff.changetype.PropertyChange;
import org.javers.core.diff.changetype.ValueChange;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;

import com.epam.monity.common.data.repository.common.model.Submission;
import com.epam.monity.common.data.repository.common.model.SubmissionAction;
import com.epam.monity.common.data.repository.util.CommonPipelineMessageFieldAccessor;
import com.epam.monity.exgester.ExgesterApplication;
import com.epam.monity.exgester.listener.DeterminationResultQueueListener;
import com.epam.monity.exgester.listener.RegservNotificationTopicListener;
import com.epam.monity.exgester.mapper.model.DeterminationPayload;
import com.epam.monity.exgester.mapper.model.HeaderData;
import com.epam.monity.exgester.mapper.model.RegServMessage;
import com.epam.monity.exgester.mapper.model.RegServNotification;
import com.epam.monity.exgester.processor.CdrRestClientStub;
import com.epam.monity.exgester.service.ClockService;
import com.epam.monity.exgester.submission.SubmissionHelper;
import com.epam.monity.internal.event.lineage.Notification;
import com.epam.monity.starter.MonitySecondaryActiveMqAutoConfiguration;
import com.epam.streaming.model.internal.CommonPipelineMessage;
import com.epam.streaming.model.internal.TechnicalData;
import com.epam.streaming.model.internal.dictionary.ReportFieldsDictionary;
import com.epam.streaming.model.internal.enumeration.DayCountFraction;
import com.epam.streaming.model.internal.enumeration.DerivativeType;
import com.epam.streaming.model.internal.enumeration.Eligible;
import com.epam.streaming.model.internal.enumeration.ExecutionVenueType;
import com.epam.streaming.model.internal.enumeration.InputType;
import com.epam.streaming.model.internal.enumeration.RegulatoryReportable;
import com.epam.streaming.model.internal.enumeration.ReportingCounterparty;
import com.epam.streaming.model.internal.enumeration.ReportingEligibility;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.markit.processing.ir.regserv.comms.configs.ResponseCategory;
import com.markit.processing.ir.regserv.comms.models.IRRegservResponse;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import net.javacrumbs.jsonunit.core.Option;
import test.utils.TestMessageProducer;

@Log4j2
@SpringBootTest(classes = {ExgesterApplication.class, MockUpiResolverClientConfig.class})
public abstract class StepDefs {

  private static final Map<String, Class<? extends Enum<?>>> ENUM_MAP = Map.ofEntries(
      Map.entry("RegulatoryReportable", RegulatoryReportable.class),
      Map.entry("PriceUnit", PriceUnit.class),
      Map.entry("ReportingEligibility", ReportingEligibility.class),
      Map.entry("ExecutionVenueType", ExecutionVenueType.class),
      Map.entry("Eligible", Eligible.class),
      Map.entry("ReportingCounterparty", ReportingCounterparty.class),
      Map.entry("DerivativeType", DerivativeType.class),
      Map.entry("InputType", InputType.class),
      Map.entry("legType", LegType.class),
      Map.entry("OptionType", OptionType.class),
      Map.entry("OptionStyle", OptionStyle.class),
      Map.entry("ClearingStatus", ClearingStatus.class),
      Map.entry("DayCountFraction", DayCountFraction.class),
      Map.entry("ClearingExceptionType", ClearingExceptionType.class),
      Map.entry("ConfirmationMethod", ConfirmationMethod.class),
      Map.entry("TimeLinessTimeStamp", TimeLinessTimeStamp.class)
  );
  private static final SimpleDateFormat DATE_FORMAT;
  private static final PropertyUtilsBean PROPERTY_UTILS_BEAN = new PropertyUtilsBean();

  static {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  protected final Javers javers = JaversBuilder.javers().build();
  protected UnaryOperator<RegServMessage> cleanMessageProducer = regServMessage -> {
    removeMessageIdField(regServMessage);
    return regServMessage;
  };
  protected Set<String> skippedProperties = Set.of("headerData.generationTimestamp", "headerData.messageId", "headerData.creationTimestamp", "headerData.tradeEvent");
  @Autowired
  protected DeterminationResultQueueListener determinationResultQueueListener;
  @Autowired
  protected RegservNotificationTopicListener regservNotificationListener;
  @Autowired
  @MockBean(name = "targetJmsTemplate")
  protected JmsTemplate targetJmsTemplate;
  @Autowired
  @Qualifier(MonitySecondaryActiveMqAutoConfiguration.SECONDARY_TOPIC_JMS_TEMPLATE)
  @MockBean
  protected JmsTemplate lineageJmsTemplate;
  @Autowired
  protected CdrRestClientStub cdrRestClientStub;
  @Autowired
  protected SubmissionHelper submissionHelper;
  @SpyBean
  @Autowired
  protected ClockService clockService;
  @SpyBean
  @Autowired
  protected AlertSender alertSender;

  @Autowired
  protected ObjectMapper objectMapper;

  @Value("${monity.message-bus.name.exgesterOutMessageBus}")
  private String monityExgesterOutMessageBus;

  protected String processedFilePath;
  protected CommonPipelineMessage commonPipelineMessage;
  protected CommonPipelineMessage persistedCommonPipelineMessage;
  protected RegServNotification regServNotificationMessage;
  protected String regime;
  protected List<String> reportTypes = new ArrayList<>();
  protected List<Notification> lineageNotifications = new ArrayList<>();
  protected UnaryOperator<String> inputMessagePathProducer;
  protected UnaryOperator<String> outputMessagePathProducer;
  protected final Map<String, Object> scenarioContext = new HashMap<>();

  protected static void removeMessageIdField(RegServMessage regServMessage) {
    regServMessage.getPayload().remove(ReportFieldsDictionary.MESSAGE_ID.getValue());
    regServMessage.getPayload().remove(ReportFieldsDictionary.REPORTING_TIMESTAMP.getValue());
  }

  // SAN has a different flow from other clients
  protected abstract List<DeterminationPayload> getDeterminationPayloads();

  @SneakyThrows
  protected void test(String inputMessageFilePath, List<String> outputMessageFilePaths) {
    test(messageFromFile(inputMessageFilePath), outputMessageFilePaths);
  }

  protected void test(CommonPipelineMessage commonPipelineMessage, List<String> outputMessageFilePaths) {
    List<RegServMessage> outputMessages = process(commonPipelineMessage).stream()
        .map(cleanMessageProducer)
        .collect(Collectors.toList());
    final List<RegServMessage> outputMessagesActual = outputMessages.stream()
      .collect(Collectors.toList());

    int outputSize = outputMessages.size();

    List<RegServMessage> expectedMessages = outputMessageFilePaths.stream()
        .map(outputMessagePathProducer)
        .flatMap(s -> TestMessageProducer.getOutputMessages(s).stream())
        .map(cleanMessageProducer).collect(Collectors.toList());
    int expectedSize = expectedMessages.size();

    List<Tuple3<RegServMessage, RegServMessage, List<Change>>> cartesianProduct = outputMessages.stream()
        .flatMap(output -> expectedMessages.stream()
            .map(expected -> new Tuple3<>(output, expected, getChanges(expected, output))))
        .sorted(Comparator.comparing(rsmrsmlt -> rsmrsmlt._3.size())).collect(Collectors.toList());

    Map<String, RegServMessage> changed = new HashMap<>();
    List<Tuple2<String, List<Change>>> matched = new ArrayList<>();
    while (!outputMessages.isEmpty() && !expectedMessages.isEmpty() && !cartesianProduct.isEmpty()) {
      Tuple3<RegServMessage, RegServMessage, List<Change>> tuple = cartesianProduct
          .get(0);
      outputMessages.remove(tuple._1);
      expectedMessages.remove(tuple._2);
      cartesianProduct.removeIf(it -> it._1 == tuple._1 || it._2 == tuple._2);
      matched.add(new Tuple2<>(tuple._2.getHeaderData().getMessageId(), tuple._3));
      if (!tuple._3.isEmpty()) {
        RegServMessage m = tuple._1;
        m.getHeaderData().setMessageId(tuple._2.getHeaderData().getMessageId());
        m.getHeaderData().setGenerationTimestamp(tuple._2.getHeaderData().getGenerationTimestamp());
        m.getHeaderData().setCreationTimestamp(tuple._2.getHeaderData().getCreationTimestamp());

        changed.put(m.getHeaderData().getMessageId(), m);
      }
    }
    writeMessages(changed, outputMessageFilePaths);
    if (!expectedMessages.isEmpty()) {
      writeAllMessages(outputMessageFilePaths, outputMessagesActual);
      fail("Expected (" + expectedSize + ") messages but received (" + outputSize
          + "), could not match message(s): " + expectedMessages
          + (matched.stream().anyMatch(it -> !it._2().isEmpty()) ? "\noutcome for the rest of messages: " + matched
          : "")
      );
    }
    if (!outputMessages.isEmpty()) {
      writeAllMessages(outputMessageFilePaths, outputMessagesActual);
      fail(
          "Received (" + outputSize + ") messages but expected (" + expectedSize
              + "), could not match message(s): "
              + expectedMessages
              + (matched.stream().anyMatch(it -> !it._2().isEmpty()) ? "\noutcome for the rest of messages: " + matched
              : "")
      );
    }
    if (!matched.isEmpty() && matched.stream().anyMatch(it -> !it._2().isEmpty())) {
      fail(format("Detected discrepancy in expected: %s and received messages: %s",
          outputMessageFilePaths, matched));
    }
  }

  @SneakyThrows
  private void writeMessages(Map<String, RegServMessage> changed, List<String> outputMessageFilePaths) {
    for (String file : outputMessageFilePaths) {
      String path = outputMessagePathProducer.apply(file);
      List<RegServMessage> fileMessages = new ArrayList<>(TestMessageProducer.getOutputMessages(path));
      boolean changesApplied = false;
      for(int i = 0; i < fileMessages.size(); ++i) {
        var fileMessage = fileMessages.get(i);
        RegServMessage correct = changed.get(fileMessage.getHeaderData().getMessageId());

        if (correct != null) {
          if (fileMessage.getPayload().get(ReportFieldsDictionary.MESSAGE_ID.getValue()) != null) {
            correct.getPayload().put(ReportFieldsDictionary.MESSAGE_ID.getValue(), fileMessage.getPayload().get(ReportFieldsDictionary.MESSAGE_ID.getValue()));
          }
          fileMessages.set(i, correct);
          changesApplied = true;
        }
      }

      if (changesApplied) {
        Path output = Paths.get("target/output", path);
        Files.createDirectories(output.getParent());
        Files.write(output, objectMapper.setDefaultPrettyPrinter(new MyPrettyPrinter(new DefaultPrettyPrinter()))
            .writerWithDefaultPrettyPrinter()
            .writeValueAsBytes(fileMessages), CREATE, WRITE);
        log.warn("Write fixed output file {} to {}", file, output);
      }
    }
  }

  @SneakyThrows
  private void writeAllMessages(List<String> outputMessageFilePaths, List<RegServMessage> outputMessagesActual) {
    if (outputMessageFilePaths.size() != 1) {
      return;
    }

    String file = outputMessageFilePaths.get(0);
    String path = outputMessagePathProducer.apply(file);
    Path output = Paths.get("target/output", path);
    Files.createDirectories(output.getParent());
    Files.write(output, objectMapper.setDefaultPrettyPrinter(new MyPrettyPrinter(new DefaultPrettyPrinter()))
      .writerWithDefaultPrettyPrinter()
      .writeValueAsBytes(outputMessagesActual), CREATE, WRITE);
  }

  protected void testLineage(List<String> lineageNotificationFilenames) {
    assertThat(lineageNotifications.size())
        .withFailMessage("Different lineage event count. Actual: %d, expected: %d",
            lineageNotifications.size(), lineageNotificationFilenames.size())
        .isEqualTo(lineageNotificationFilenames.size());
    for (String lineageFilename : lineageNotificationFilenames) {
      Notification expectedNotification = getSampleLineageMessage(outputMessagePathProducer.apply(lineageFilename));
      assertThat(lineageNotifications).anySatisfy(actual -> testLineageNotification(actual, expectedNotification));
    }
  }

  private void testLineageNotification(Notification actual, Notification expected) {
    assertThatJson(actual)
        .whenIgnoringPaths(
            "$.identifiers.notificationId",
            "$.identifiers.submissionId",
            "$.rows.*.target[?(@.label=='messageId')].value",
            "$.rows.*.sources[?(@.label=='Message ID')].value",
            "$.rows.*.target[?(@.label=='Message ID')].value",
            "$.rows.*.target[?(@.label=='generationTimestamp')].value"
        )
        .when(path("rows"), then(Option.IGNORING_ARRAY_ORDER))
        .isEqualTo(expected);
  }

  @SneakyThrows
  protected void saveLineage(String lineageNotificationFilenamePattern) {
    for (int i = 0; i < lineageNotifications.size(); i++) {
      Path savePath = Paths.get("src/test/resources/")
          .resolve(outputMessagePathProducer.apply(format(lineageNotificationFilenamePattern, i + 1)));
      Files.createDirectories(savePath.getParent());
      try (BufferedWriter writer = Files.newBufferedWriter(savePath, CREATE, WRITE, TRUNCATE_EXISTING)) {
        writer.write(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineageNotifications.get(i)));
        log.info("Lineage notification saved to: '{}'", savePath);
      }
    }
  }

  @NotNull
  private List<Change> getChanges(RegServMessage expected, RegServMessage actual) {
    List<Change> changes = new ArrayList<>(javers.compare(expected, actual).getChanges());
    changes.removeIf(change -> (change instanceof ValueChange) && skippedProperties
        .contains(((PropertyChange) change).getPropertyNameWithPath()));
    return changes;
  }

  protected List<RegServMessage> process(String inputMessageFilePath) throws IOException {
    return this.process(messageFromFile(inputMessageFilePath));
  }

  @SneakyThrows
  protected CommonPipelineMessage messageFromFile(final String inputMessageFilePath) {
    return TestMessageProducer.getMessage(inputMessagePathProducer.apply(inputMessageFilePath));
  }

  protected List<RegServMessage> process(CommonPipelineMessage pipelineMessage) {
    ArgumentCaptor<RegServMessage> captor = ArgumentCaptor.forClass(RegServMessage.class);
    ArgumentCaptor<Notification> lineageCaptor = ArgumentCaptor.forClass(Notification.class);
    ArgumentCaptor<CommonPipelineMessage> captorCommonPipelineMessage =
        ArgumentCaptor.forClass(CommonPipelineMessage.class);
    determinationResultQueueListener.listen(pipelineMessage);

    verify(targetJmsTemplate, atLeast(0))
        .convertAndSend(eq(monityExgesterOutMessageBus), captor.capture(), any(MessagePostProcessor.class));
    verify(targetJmsTemplate, atLeast(0))
        .convertAndSend(anyString(), captorCommonPipelineMessage.capture(), any(MessagePostProcessor.class));

    List<RegServMessage> sentRegServMessages = captor.getAllValues();
    persistedCommonPipelineMessage = !captorCommonPipelineMessage.getAllValues().isEmpty() ? captorCommonPipelineMessage.getValue() : null;

    for (RegServMessage message : sentRegServMessages) {
      regime = message.getHeaderData().getRegime();
      reportTypes.add(message.getHeaderData().getReportType());
    }

    verify(lineageJmsTemplate, atLeast(0)).convertAndSend(anyString(), lineageCaptor.capture(), any(MessagePostProcessor.class));
    lineageNotifications.addAll(lineageCaptor.getAllValues());
    return sentRegServMessages;

  }

  public void verifyDeterminationPayloadWithBlankPreviousSubmission(String currentMessageId, String regime, String messageType) {
    Optional<DeterminationPayload> determinationPayload = getGeneratedDeltaPayload(currentMessageId, regime, messageType);
    assertTrue(determinationPayload.isPresent());
    assertNull(determinationPayload.get().getLastSubmission());
  }

  public void verifyDeterminationPayloadWithPreviousSubmissionHavingCdmWithMessageId(String currentMessageId, String regime, String messageType, String messageIdOfPreviousSubmission) {
    Optional<DeterminationPayload> determinationPayload = getGeneratedDeltaPayload(currentMessageId, regime, messageType);
    assertTrue(determinationPayload.isPresent());
    assertEquals(determinationPayload.get().getLastSubmission().getMessage().getTechnicalData().getMessageId(), messageIdOfPreviousSubmission);
  }

  private Optional<DeterminationPayload> getGeneratedDeltaPayload(String currentMessageId, String regime, String messageType) {
    return getDeterminationPayloads().stream()
        .filter(payload -> payload.getRegime().equals(regime))
        .filter(payload -> payload.getReportType().equals(messageType))
        .filter(payload -> payload.getMessage().getTechnicalData().getMessageId().equals(currentMessageId))
        .findFirst();
  }

  protected List<RegServMessage> process(RegServNotification notification) {
    ArgumentCaptor<RegServMessage> captor = ArgumentCaptor.forClass(RegServMessage.class);
    ArgumentCaptor<Notification> lineageCaptor = ArgumentCaptor.forClass(Notification.class);
    regservNotificationListener.listen(notification);

    verify(targetJmsTemplate, atLeast(0))
        .convertAndSend(anyString(), captor.capture(), any(MessagePostProcessor.class));

    List<RegServMessage> sentRegServMessages = captor.getAllValues();

    for (RegServMessage message : sentRegServMessages) {
      regime = message.getHeaderData().getRegime();
      reportTypes.add(message.getHeaderData().getReportType());
    }

    verify(lineageJmsTemplate, atLeast(0)).convertAndSend(anyString(), lineageCaptor.capture());
    lineageNotifications.addAll(lineageCaptor.getAllValues());
    return sentRegServMessages;
  }

  @SneakyThrows
  public void verifyPersistedCommonPipelineMessage(List<Map<String, String>> expectedFields) {
    Assertions.assertThat(persistedCommonPipelineMessage)
        .withFailMessage("Expected message to be persisted, but it wasn't")
        .isNotNull();

    for (Map<String, String> fieldDefinition : expectedFields) {
      String nameDefinition = fieldDefinition.get("name");
      String valueDefinition = fieldDefinition.get("value");
      String classDefinition = fieldDefinition.get("class");
      Object value = null;
      if (nonNull(valueDefinition)) {
        switch (classDefinition) {
          case "boolean":
            value = Boolean.valueOf(valueDefinition);
            break;
          case "string":
            value = valueDefinition;
            if ("<<EMPTY>>".equals(value)) {
              value = "";
            }
            break;
          case "int":
            value = Integer.valueOf(valueDefinition);
            break;
          case "bigdecimal":
            value = new BigDecimal(valueDefinition);
            break;
          case "date":
            value = DATE_FORMAT.parse(valueDefinition);
            break;
          default:
            value = Optional.ofNullable(ENUM_MAP.get(classDefinition))
                .map(
                    enumType -> Arrays.stream(enumType.getEnumConstants())
                        .filter(enumConstant -> enumConstant.name().equals(valueDefinition))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("No contant '" + valueDefinition + "' for enum class: " + classDefinition))
                )
                .orElseThrow(() -> new IllegalArgumentException("Cannot find definition class for: " + classDefinition));
        }
      }
      String payloadDefinition = fieldDefinition.get("fieldType");
      Object actualValue;
      switch (payloadDefinition) {
        case "processingPayload":
          actualValue = persistedCommonPipelineMessage.getPayload().getProcessingPayload().getField(nameDefinition);
          Assertions.assertThat(value)
              .as("Expected Processing field '%s'", nameDefinition)
              .isEqualTo(actualValue);
          break;
        case "commonReportPayload":
          if (value == null) {
            Assertions.assertThat((Object) persistedCommonPipelineMessage.getPayload().getCommonReportPayload().getField(nameDefinition))
                .as("Expected CDM field '%s'", nameDefinition)
                .isNull();
          } else {
            Assertions.assertThat((Object) persistedCommonPipelineMessage.getPayload().getCommonReportPayload().getField(nameDefinition))
                .as("Expected CDM field '%s'", nameDefinition)
                .isEqualTo(value);
          }
          break;
        default:
          throw new IllegalArgumentException("Cannot find definition payload for: " + payloadDefinition);
      }
    }
  }


  @SneakyThrows
  public void modifyMessageFields(List<Map<String, String>> modifiedFields) {
    for (Map<String, String> fieldDefinition : modifiedFields) {
      String nameDefinition = fieldDefinition.get("name");
      String valueDefinition = fieldDefinition.get("value");
      String classDefinition = fieldDefinition.get("class");
      Object value = null;
      if (nonNull(valueDefinition)) {
        switch (classDefinition) {
          case "null":
            value = null;
            break;
          case "boolean":
            value = Boolean.valueOf(valueDefinition);
            break;
          case "string":
            value = valueDefinition;
            if ("<<EMPTY>>".equals(value)) {
              value = "";
            }
            break;
          case "int":
            value = Integer.valueOf(valueDefinition);
            break;
          case "bigdecimal":
            value = new BigDecimal(valueDefinition);
            break;
          case "date":
            value = DATE_FORMAT.parse(valueDefinition);
            break;
          default:
            value = Optional.ofNullable(ENUM_MAP.get(classDefinition))
                .map(
                    enumType -> Arrays.stream(enumType.getEnumConstants())
                        .filter(enumConstant -> enumConstant.name().equals(valueDefinition))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("No contant '" + valueDefinition + "' for enum class: " + classDefinition))
                )
                .orElseThrow(() -> new IllegalArgumentException("Cannot find definition class for: " + classDefinition));
        }
      }
      String payloadDefinition = fieldDefinition.get("fieldType");
      switch (payloadDefinition) {
        case "processingPayload":
          this.commonPipelineMessage.getPayload().getProcessingPayload().addField(nameDefinition, value);
          break;
        case "payloadAdditionalFields":
          if (this.commonPipelineMessage.getTechnicalData().getPayloadAdditionalFields() == null) {
            this.commonPipelineMessage.getTechnicalData().setPayloadAdditionalFields(new HashMap<>());
          }
          this.commonPipelineMessage.getTechnicalData().getPayloadAdditionalFields().put(nameDefinition, value);
          break;
        case "commonReportPayload":
          ofNullable(value).ifPresentOrElse(
              val -> this.commonPipelineMessage.getPayload().getCommonReportPayload().addField(nameDefinition, val),
              () -> this.commonPipelineMessage.getPayload().getCommonReportPayload().removeField(nameDefinition));
          break;
        case "messageId":
          this.commonPipelineMessage.getTechnicalData().setMessageId(String.valueOf(value));
          this.commonPipelineMessage.getTechnicalData().setCdmMessageId(String.valueOf(value));
          break;
        case "otherPartyPayment":
          if (value == null) {
            this.commonPipelineMessage.getPayload().getAdditionalFields().getFields().remove("otherPartyPayment");
          } else {
            String fileContent = Resources.toString(Resources.getResource(String.valueOf(value)), StandardCharsets.UTF_8);
            List<String> otherPartyPayment = objectMapper.readValue(fileContent, new TypeReference<>() {});

            this.commonPipelineMessage.getPayload().getAdditionalFields().addField("otherPartyPayment", otherPartyPayment);
          }
          break;
        case "rolledBackToLegType":
          this.commonPipelineMessage.getPayload().getProcessingPayload().setRolledBackToLegType(String.valueOf(value));
          break;
        default:
          throw new IllegalArgumentException("Cannot find definition payload for: " + payloadDefinition);
      }
    }
  }

  @SneakyThrows
  public void modifyNotificationFields(List<Map<String, String>> modifiedFields) {
    for (Map<String, String> fieldDefinition : modifiedFields) {
      String nameDefinition = fieldDefinition.get("name");
      String valueDefinition = maybeContextValue(fieldDefinition.get("value"));
      String classDefinition = fieldDefinition.get("class");
      Object value = null;
      if (nonNull(valueDefinition)) {
        switch (classDefinition) {
          case "string":
            value = valueDefinition;
            break;
          case "ResponseCategory":
            value = ResponseCategory.valueOf(valueDefinition);
            break;
          default:
            throw new IllegalArgumentException("Cannot find definition class for: " + classDefinition);
        }
      }
      String fieldType = fieldDefinition.get("fieldType");
      switch (fieldType) {
        case "headerData":
          final HeaderData headerData = this.regServNotificationMessage.getHeaderData();
          FieldUtils.writeField(headerData, nameDefinition, value, true);
          break;
        case "responseMap":
          final IRRegservResponse responseMap = this.regServNotificationMessage.getResponseMap();
          FieldUtils.writeField(responseMap, nameDefinition, value, true);
          break;
        case "trErrorMap":
          this.regServNotificationMessage.getResponseMap().getTrErrorMap().put(nameDefinition, String.valueOf(value));
          break;
        case "trErrorMap2":
          this.regServNotificationMessage.getResponseMap().getTrErrorMap2().put(nameDefinition, String.valueOf(value));
          break;
        default:
          throw new IllegalArgumentException("Cannot find fieldType for: " + fieldType);
      }
    }
  }

  private String maybeContextValue(String value) {
    if (nonNull(value) && value.matches("\\{.*}")) {
      return String.valueOf(this.scenarioContext.get(value.replaceAll("[{}]","")));
    }
    return value;
  }

  public void storeContextKey(String submissionField, String contextKey, List<Map<String, String>> submissionParams){
    final Map<String, String> submissionParam = submissionParams.get(0);
    final String transactionId = ofNullable(this.commonPipelineMessage)
        .map(submissionHelper::determineTransactionId).orElse(null);
    List<Submission> submissions = cdrRestClientStub.findSubmissions(submissionParam.get("regime"),
        submissionParam.get("reportType"), submissionParam.get("referenceMessageId"), transactionId, null,
        parseBoolean(submissionParam.get("referencePrevious")),
        parseBoolean(submissionParam.get("phantomPrevious")),
        parseBoolean(submissionParam.get("isSuperseded")), submissionParam.get("submittedFor"));
    assertThat(submissions).hasSize(1);
    final Submission submission = submissions.get(0);
    switch (submissionField) {
      case "messageId":
        scenarioContext.put(contextKey, submission.getMessageID());
        break;
      default:
        throw new IllegalArgumentException("Cannot find strategy to retrieve: " + submissionField);
    }
  }

  @SneakyThrows
  public void assertModifiedMessageSuppressed(List<Map<String, String>> suppressSubmissions) {
    assertThat(process(this.commonPipelineMessage)).isEmpty();
    andAssertModifiedMessageSuppressed(suppressSubmissions);
  }

  @SneakyThrows
  public void assertModifiedMessageNoOutput() {
    List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertThat(regServMessages).isEmpty();
    Collection<Submission> submissions = cdrRestClientStub.findSubmissionForTransactionId(this.commonPipelineMessage);
    assertThat(submissions).isEmpty();
  }

  @SneakyThrows
  public void assertModifiedMessageNoOutputCurrentMessage() {
    List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertThat(regServMessages).isEmpty();
    Collection<Submission> submissions = cdrRestClientStub
        .findSubmissionForTransactionIdCurrent(this.commonPipelineMessage,
            not(submission -> SUPPRESS.equals(submission.getSubmissionStatus())));
    assertThat(submissions).isEmpty();
  }

  @SneakyThrows
  public void assertModifiedMessageNoOutputToRegserv() {
    List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertThat(regServMessages).isEmpty();
  }

  protected void andAssertModifiedMessageSuppressed(List<Map<String, String>> suppressSubmissions) {
    final boolean isFeeTrade = checkIfNovationStepFeeTrade();

    for (int i = 0; i < suppressSubmissions.size(); ++i) {
      Map<String, String> suppressSubmission = suppressSubmissions.get(i);
      final String transactionId = ofNullable(suppressSubmission.get("transactionId"))
          .filter(StringUtils::isNotBlank)
          .or(() -> ofNullable(this.commonPipelineMessage).map(submissionHelper::determineTransactionId))
          .orElse(null);
      SubmissionSearchRequest request = SubmissionSearchRequest.builder()
          .assetClass(suppressSubmission.get("assetClass"))
          .regime(suppressSubmission.get("regime"))
          .reportType(suppressSubmission.get("reportType"))
          .transactionId(transactionId)
          .isFeeTrade(isFeeTrade)
          .ignoreLce(Boolean.parseBoolean(suppressSubmission.get("ignoreLce")))
          .submittedForParty(suppressSubmission.get("submittedForParty"))
          .build();

      Optional.ofNullable(suppressSubmission.get("fxLegType")).ifPresent(
          val -> request.setFxLegType(LegType.valueOf(val).getCodeValue()));

      Optional<Submission> lastSubmission = cdrRestClientStub.findLastSubmission(request);
      final Submission submission = lastSubmission
          .orElseThrow(() -> new IllegalArgumentException("Can't find submission for params: " + suppressSubmission));
      assertAll(
          "Row: " + i,
          () -> assertThat(submission.getSubmissionStatus()).isEqualTo("Suppress"),
          () -> assertThat(submission.getTrReportSuppressionReason())
              .isEqualTo(suppressSubmission.get("suppressMessage")),
          () -> {
            if (suppressSubmission.containsKey("cdmMessageId")) {
              String messageId = suppressSubmission.get("cdmMessageId");
              assertThat(submission.getMessage().getTechnicalData().getMessageId()).isEqualTo(messageId);
            }
          },
          () -> {
            if (suppressSubmission.containsKey("submittedForParty")) {
              String submittedForParty = suppressSubmission.get("submittedForParty");
              assertThat(submission.getSubmittedForParty()).isEqualTo(submittedForParty);
            }
          }
      );
    }
  }

  private boolean checkIfNovationStepFeeTrade() {
    final String tradeEvent = CommonPipelineMessageFieldAccessor
        .getCommonReportPayloadValue(this.commonPipelineMessage, ReportFieldsDictionary.TRADE_EVENT);
    return isFeeTrade(tradeEvent);
  }

  protected void andAssertModifiedMessageSubmitted(final List<Map<String, String>> submissionParams) {
    final List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertSubmittedRegServMessages(submissionParams, regServMessages);
  }

  protected void andAssertModifiedMessageNoRegservSubmissionStored(final List<Map<String, String>> submissionParams) {
    final List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertThat(regServMessages).isEmpty();
    assertSubmissionsStored(submissionParams);
  }

  protected void andAssertNotificationSubmitted(final List<Map<String, String>> submissionParams) {
    final List<RegServMessage> regServMessages = process(this.regServNotificationMessage);
    assertSubmittedRegServMessages(submissionParams, regServMessages);
  }

  protected void andAssertNotificationSubmittedWithSdmParams(final List<Map<String, String>> sdmParams) {
    final List<RegServMessage> regServMessages = process(this.regServNotificationMessage);
    assertRegservMessagesFields(regServMessages, sdmParams);
  }

  protected void andProcessRegservNotification() {
    process(this.regServNotificationMessage);
  }

  public void replayCdmByMessageId(String messageId, String newAutoGeneratedMessageId) {
    triggerUserAction("REPLAY", messageId, newAutoGeneratedMessageId);
  }

  public void backloadCdmByMessageId(String messageId, String newAutoGeneratedMessageId) {
    triggerUserAction("BACKLOAD", messageId, newAutoGeneratedMessageId);
  }

  public void triggerUserAction(String userAction, String messageId, String newAutoGeneratedMessageId) {
    this.commonPipelineMessage = cdrRestClientStub.getCdmByMessageId(messageId)
        .map(this::copyMessage)
        .map(cdm -> {
          cdm.getTechnicalData().setMessageId(newAutoGeneratedMessageId);
          cdm.getTechnicalData().setCdmMessageId(newAutoGeneratedMessageId);
          cdm.getTechnicalData().getTrafficDescriptor().setTrafficType(userAction);
          return cdm;
        })
        .orElseThrow(() -> new IllegalArgumentException("No CDM with message id: " + messageId));
  }

  public void mockCurrentDateTimeUtc(final LocalDateTime localDateTime) {
    when(clockService.getCurrentDateTimeUtc()).thenReturn(localDateTime);
  }

  @SneakyThrows
  private CommonPipelineMessage copyMessage(CommonPipelineMessage message) {
    // to avoid updating same instance
    return objectMapper.readValue(objectMapper.writeValueAsString(message), CommonPipelineMessage.class);
  }

  private void assertSubmittedRegServMessages(final List<Map<String, String>> submissionParams,
      final List<RegServMessage> regServMessages) {
    assertThat(regServMessages).hasSize(submissionParams.size());
    assertSubmissionsStored(submissionParams);
  }

  private void assertSubmissionsStored(final List<Map<String, String>> submissionParams) {
    final String transactionId = ofNullable(this.commonPipelineMessage)
        .map(submissionHelper::determineTransactionId).orElse(null);
    final String messageId = ofNullable(this.commonPipelineMessage)
        .map(CommonPipelineMessage::getTechnicalData).map(TechnicalData::getMessageId).orElse(null);
    for (int i = 0; i < submissionParams.size(); i++) {
      Map<String, String> submissionParam = submissionParams.get(i);
      final String referenceMessageId = ofNullable(submissionParam.get("referenceMessageId")).orElse(messageId);
      List<Submission> submissions = null;

      if (submissionParam.get("cdmMessageId") != null) {
        submissions = cdrRestClientStub.findSubmissions(submissionParam.get("cdmMessageId"),
            submissionParam.get("regime"), submissionParam.get("reportType"), submissionParam.get("action"),
            submissionParam.get("lifecycleEventType"));
      } else {
        submissions = cdrRestClientStub.findSubmissions(submissionParam.get("regime"),
            submissionParam.get("reportType"), referenceMessageId, transactionId, null,
            parseBoolean(submissionParam.get("referencePrevious")),
            parseBoolean(submissionParam.get("phantomPrevious")),
            parseBoolean(submissionParam.get("isSuperseded")),
            submissionParam.get("submittedFor"));
      }

      assertThat(submissions).hasSize(1);
      final Submission submission = submissions.get(0);
      assertAll(
          String.format("Assert row: %s. Actual - regime=%s, reportType=%s, action=%s, lifecycleEventType=%s, transactionType=%s, submittedFor=%s, party1ActionType=%s, party2ActionType=%s, tradeParty1Lei=%s, reportingEligibility=%s",
              i, submission.getTradeParty1Jurisdiction(), submission.getMessageType(), submission.getSubmissionAction(), submission.getLifecycleEventType(), submission.getTransactionType(), submission.getSubmittedForParty(),
              submission.getTradeParty1RegulatoryActionType(), submission.getTradeParty2RegulatoryActionType(), submission.getTradeParty1Lei(),
              submission.getReportingEligibilityStatus()),
          () -> assertSubmissionValue(submissionParam, "action",
              ofNullable(submission.getSubmissionAction()).map(SubmissionAction::getActionName).orElse(null)),
          () -> assertSubmissionValue(submissionParam, "lifecycleEventType", submission.getLifecycleEventType()),
          () -> assertSubmissionValue(submissionParam, "transactionType", submission.getTransactionType()),
          () -> assertSubmissionValue(submissionParam, "submittedFor", submission.getSubmittedForParty()),
          () -> assertSubmissionValue(submissionParam, "party1ActionType",
              submission.getTradeParty1RegulatoryActionType()),
          () -> assertSubmissionValue(submissionParam, "party2ActionType",
              submission.getTradeParty2RegulatoryActionType()),
          () -> assertSubmissionValue(submissionParam, "tradeParty1Lei", submission.getTradeParty1Lei()),
          () -> assertSubmissionValue(submissionParam, "reportingEligibility", submission.getReportingEligibilityStatus()),
          () -> assertSubmissionValue(submissionParam, "timelinessStatus", submission.getTimelinessStatus()),
          () -> assertSubmissionValue(submissionParam, "productType", submission.getProductType())
      );
    }
  }

  private void assertSubmissionValue(Map<String, String> params, Object paramKey, Object submissionValue) {
    final String value = params.get(paramKey);

    if (nonNull(value)) {
      assertThat(String.valueOf(submissionValue))
          .withFailMessage("Key: %s should be equal to %s but was %s", paramKey, value, String.valueOf(submissionValue))
          .isEqualTo(value);
    } else if ("null".equals(value)) {
      assertThat(String.valueOf(submissionValue))
          .withFailMessage("Key: %s should be 'null' but was %s", paramKey, value, String.valueOf(submissionValue))
          .isNull();
    }
  }

  /**
   *
   * example List.of(Map.of("headerData.action", "New"), Map.of("headerData.regime", "CFTC"))
   *
   * @param sdmParams List<Map<FIELD_PATH, EXPECTED_VALUE>>
   */
  protected void assertRegservMessagesFields(final List<Map<String, String>> sdmParams) {
    final List<RegServMessage> regServMessages = process(this.commonPipelineMessage);
    assertRegservMessagesFields(regServMessages, sdmParams);
  }

  public void checkOutgoingMessagesForUserAction(final List<Map<String, String>> sdmParams) {
    ArgumentCaptor<RegServMessage> captor = ArgumentCaptor.forClass(RegServMessage.class);
    verify(targetJmsTemplate, atLeast(0))
        .convertAndSend(eq(monityExgesterOutMessageBus), captor.capture(), any(MessagePostProcessor.class));

    List<RegServMessage> regServMessages = captor.getAllValues();
    assertRegservMessagesFields(regServMessages, sdmParams);
  }

  private void assertRegservMessagesFields(List<RegServMessage> regServMessages, final List<Map<String, String>> sdmParams) {
    assertThat(regServMessages).hasSameSizeAs(sdmParams);

    for (int i = 0; i < regServMessages.size(); i++) {
      RegServMessage message = regServMessages.get(i);
      Executable[] executables = sdmParams.get(i).entrySet().stream()
          .map(entry -> assertFieldValue(getFieldValue(message, entry.getKey()), entry.getValue(), entry.getKey()))
          .toArray(Executable[]::new);
      HeaderData header = message.getHeaderData();

      assertAll(
          "Row: " + i + ". Should match regserv message for regime: " + header.getRegime() + ", report type: " + header.getReportType() + ", action: " + header.getAction(),
          executables
      );
    }
  }

  public void markAllSubmissionsAsAcked() {
    cdrRestClientStub.markAllSubmissionsAsAcked();
  }

  public void markAllSubmissionsAsNacked() {
    cdrRestClientStub.markAllSubmissionsAsNacked();
  }

  public void shouldSendAlerts(List<Map<String, String>> expectedValues) {
    ArgumentCaptor<NewAlert> captor = ArgumentCaptor.forClass(NewAlert.class);
    verify(alertSender, atLeastOnce()).send(captor.capture());

    final List<NewAlert> alerts = captor.getAllValues();
    assertThat(alerts).hasSameSizeAs(expectedValues);

    for (int i = 0; i < expectedValues.size(); i++) {
      NewAlert alert = alerts.get(i);
      Executable[] executables = expectedValues.get(i).entrySet().stream()
          .map(entry -> assertFieldValue(getFieldValue(alert, entry.getKey()), entry.getValue(), entry.getKey()))
          .toArray(Executable[]::new);

      assertAll("Row: " + i + ". Should match alert : " + alert, executables);
    }
  }

  public void setProcessedBeforeRewrite(boolean processedBeforeRewrite) {
    cdrRestClientStub.isProcessedTransactionExistsBeforeDateForTransactionId = processedBeforeRewrite;
  }

  protected void checkSubmissionsStored(final List<Map<String, String>> submissionParams) {
    assertSubmissionsStored(submissionParams);
  }

  private static Executable assertFieldValue(String actualValue, String expectedValue, String property) {
    return () -> {
      if (StringUtils.isBlank(expectedValue)) {
        assertThat(actualValue)
            .withFailMessage("Field: %s should be null or empty but was %s", property, actualValue)
            .isNullOrEmpty();
      } else {
        assertThat(actualValue)
            .withFailMessage("Field: %s should be equal to %s but was %s", property, expectedValue, actualValue)
            .isEqualTo(expectedValue);
      }
    };
  }

  @SneakyThrows
  private static String getFieldValue(Object object, String path) {
    return Objects.toString(PROPERTY_UTILS_BEAN.getProperty(object, path), "");
  }

  public static class MyPrettyPrinter extends DefaultPrettyPrinter {

    public MyPrettyPrinter(DefaultPrettyPrinter base) {
      super(base);
      this.indentArraysWith(new DefaultIndenter());
      this.indentObjectsWith(new DefaultIndenter());
    }

    @Override
    public MyPrettyPrinter createInstance() {
      if (getClass() != MyPrettyPrinter.class) {
        throw new IllegalStateException("Failed `createInstance()`: " + getClass().getName()
            + " does not override method; it has to");
      }
      return new MyPrettyPrinter(this);
    }

    public DefaultPrettyPrinter withSeparators(Separators separators) {
      this._separators = separators;
      this._objectFieldValueSeparatorWithSpaces = separators.getObjectFieldValueSeparator() + " ";
      return this;
    }

  }
}
