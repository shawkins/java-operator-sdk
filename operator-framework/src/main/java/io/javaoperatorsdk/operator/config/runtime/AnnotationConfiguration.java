package io.javaoperatorsdk.operator.config.runtime;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.ControllerUtils;
import io.javaoperatorsdk.operator.api.Controller;
import io.javaoperatorsdk.operator.api.ResourceController;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.processing.event.internal.CustomResourceEventFilter;
import io.javaoperatorsdk.operator.processing.event.internal.CustomResourceEventFilters;

public class AnnotationConfiguration<R extends CustomResource>
    implements ControllerConfiguration<R> {

  private final ResourceController<R> controller;
  private final Optional<Controller> annotation;
  private ConfigurationService service;

  public AnnotationConfiguration(ResourceController<R> controller) {
    this.controller = controller;
    this.annotation = Optional.ofNullable(controller.getClass().getAnnotation(Controller.class));
  }

  @Override
  public String getName() {
    return ControllerUtils.getNameFor(controller);
  }

  @Override
  public String getFinalizer() {
    return annotation
        .map(Controller::finalizerName)
        .filter(Predicate.not(String::isBlank))
        .orElse(ControllerUtils.getDefaultFinalizerName(getCRDName()));
  }

  @Override
  public boolean isGenerationAware() {
    return annotation.map(Controller::generationAwareEventProcessing).orElse(true);
  }

  @Override
  public Class<R> getCustomResourceClass() {
    return RuntimeControllerMetadata.getCustomResourceClass(controller);
  }

  @Override
  public Set<String> getNamespaces() {
    return Set.of(annotation.map(Controller::namespaces).orElse(new String[] {}));
  }

  @Override
  public String getLabelSelector() {
    return annotation.map(Controller::labelSelector).orElse("");
  }

  @Override
  public ConfigurationService getConfigurationService() {
    return service;
  }

  @Override
  public void setConfigurationService(ConfigurationService service) {
    this.service = service;
  }

  @Override
  public String getAssociatedControllerClassName() {
    return controller.getClass().getCanonicalName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public CustomResourceEventFilter<R> getEventFilter() {
    CustomResourceEventFilter<R> answer = null;

    var filterTypes = annotation.map(Controller::eventFilters);
    if (filterTypes.isPresent()) {
      for (var filterType : filterTypes.get()) {
        try {
          CustomResourceEventFilter<R> filter = filterType.getConstructor().newInstance();

          if (answer == null) {
            answer = filter;
          } else {
            answer = filter.and(filter);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    return answer != null
        ? answer
        : CustomResourceEventFilters.passthrough();
  }
}

